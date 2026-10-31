package block

import "context"

// MarkStored reports, for each ref, whether the buffer holds it pending while
// the store below already holds it, and marks each such entry counted, so its
// write adds no shared count: the caller counts the holder it adds instead.
//
// A ref the buffer wrote below reports false, though the store holds it: that
// write was the writer's own, and the store counted it. So does a ref the
// buffer holds as a tombstone, has lent to a publication, or does not hold. A
// buffer that does not record its writes cannot tell its own, and reports
// false for every ref. It waits for a drain in progress and holds off the next.
func (s *BufferedStore) MarkStored(ctx context.Context, refs []*BlockRef) ([]bool, error) {
	// Encode the keys before taking the locks.
	out := make([]bool, len(refs))
	keys := make([]string, len(refs))
	for i, ref := range refs {
		key, err := marshalRefKey(ref)
		if err != nil {
			return nil, err
		}
		keys[i] = key
	}

	// Hold off drains, so no probed entry is written below during the probe.
	release, err := s.drainMu.Lock(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	// Select the pending entries this buffer has not written below.
	var probe []*BlockRef
	var probeIdx []int
	var entries []*pendingBlock
	s.bcast.HoldLock(func(_ func(), _ func() <-chan struct{}) {
		if s.written == nil {
			return
		}
		for i, key := range keys {
			p := s.pending[key]
			if p == nil || p.tombstone || p.borrowed {
				continue
			}
			if _, ok := s.written[key]; ok {
				continue
			}
			probe = append(probe, p.ref)
			probeIdx = append(probeIdx, i)
			entries = append(entries, p)
		}
	})
	if len(probe) == 0 {
		return out, nil
	}

	// Ask the store below.
	found, err := s.inner.GetBlockExistsBatch(s.innerReadContext(ctx), probe)
	if err != nil {
		return nil, err
	}

	// Mark each found entry a write has not replaced since.
	s.bcast.HoldLock(func(_ func(), _ func() <-chan struct{}) {
		for j, ok := range found {
			i := probeIdx[j]
			if !ok || s.pending[keys[i]] != entries[j] || entries[j].borrowed {
				continue
			}
			entries[j].counted = true
			out[i] = true
		}
	})
	return out, nil
}
