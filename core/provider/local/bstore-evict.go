package provider_local

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	account_settings "github.com/s4wave/spacewave/core/account/settings"
	"github.com/s4wave/spacewave/db/block"
	block_store_writeback "github.com/s4wave/spacewave/db/block/store/writeback"
	"github.com/s4wave/spacewave/net/hash"
)

// readSetMax bounds the blocks a read set records in one generation. Reads
// past it go unrecorded, so their blocks age as if unread.
const readSetMax = 1 << 20

// readKeepAge is how long a read keeps its block from eviction, at least. A
// pass can follow the last within seconds, so a set taken at each pass would
// keep a block only for those seconds, and the blocks a World reads every few
// minutes would leave and be read back from the backend each time.
const readKeepAge = 10 * time.Minute

// readSet records the blocks read from the local store, which eviction keeps
// for at least readKeepAge after the read. It holds two generations: the
// reads since the last turn and the ones before it.
type readSet struct {
	// enabled is set while the placement has a resident budget, before the
	// budget reaches the store, so reads after it are recorded.
	enabled atomic.Bool
	// mtx guards the fields below.
	mtx sync.Mutex
	// hashes holds the raw hashes of the blocks read since turned.
	hashes map[string]struct{}
	// prev holds the hashes of the generation before.
	prev map[string]struct{}
	// turned is when hashes started, zero before the first pass.
	turned time.Time
}

// add records a read of the block with hash h.
func (r *readSet) add(h *hash.Hash) {
	// Record nothing without a budget.
	if !r.enabled.Load() {
		return
	}

	// Record the hash while the set has room.
	r.mtx.Lock()
	defer r.mtx.Unlock()
	if r.hashes == nil {
		r.hashes = make(map[string]struct{})
	}
	if len(r.hashes) < readSetMax {
		r.hashes[string(h.GetHash())] = struct{}{}
	}
}

// take returns a keep test for an eviction pass: it keeps the blocks read in
// both generations, so every block read within readKeepAge. It turns the
// generations once the current one is readKeepAge old.
func (r *readSet) take() func(*hash.Hash) bool {
	// Turn the generations when the current one is old enough, timing the
	// first from the first pass.
	r.mtx.Lock()
	switch now := time.Now(); {
	case r.turned.IsZero():
		r.turned = now
	case now.Sub(r.turned) >= readKeepAge:
		r.prev, r.hashes, r.turned = r.hashes, nil, now
	}
	r.mtx.Unlock()

	// Keep the blocks either generation recorded.
	return func(h *hash.Hash) bool {
		// Look the hash up in both generations.
		key := h.GetHash()
		r.mtx.Lock()
		defer r.mtx.Unlock()
		_, cur := r.hashes[string(key)]
		_, prev := r.prev[string(key)]
		return cur || prev
	}
}

// readRecorder records the blocks the local store serves to reads.
type readRecorder struct {
	block.StoreOps
	// reads records the blocks served.
	reads *readSet
}

// GetBlock reads the block and records a hit.
func (r *readRecorder) GetBlock(ctx context.Context, ref *block.BlockRef) ([]byte, bool, error) {
	data, found, err := r.StoreOps.GetBlock(ctx, ref)
	if found {
		r.reads.add(ref.GetHash())
	}
	return data, found, err
}

// GetStoredBlock reads the block with its refs and records a hit.
func (r *readRecorder) GetStoredBlock(ctx context.Context, ref *block.BlockRef) (*block.StoredBlock, error) {
	stored, err := r.StoreOps.GetStoredBlock(ctx, ref)
	if stored != nil {
		r.reads.add(ref.GetHash())
	}
	return stored, err
}

// setPolicy applies the cache settings of the placement. Reads are recorded
// from before the store sees a resident budget, so no read the budget pass
// should keep goes unrecorded.
func (p *placementState) setPolicy(cache *account_settings.BlockStoreCache) {
	policy := uploadPolicy(cache)
	p.reads.enabled.Store(policy.ResidentHigh > 0)
	p.wb.SetPolicy(policy)
}

// runEviction keeps the store's resident bytes under its placement's budget.
// When the resident bytes cross the high mark, a pass drops the uploaded
// blocks not read since the last pass, oldest queued first, down to the low
// mark. Uploads and writes change the status, which starts the next check.
func (p *placementState) runEviction(ctx context.Context) error {
	for {
		// Run a pass while the store is past its budget.
		status, changed := p.wb.GetStatus()
		if want := status.EvictBytes(); want > 0 {
			if _, err := p.evict(ctx, want); err != nil {
				return err
			}
		}

		// Wait for the next status change.
		select {
		case <-ctx.Done():
			return context.Canceled
		case <-changed:
		}
	}
}

// evict drops up to maxBytes of uploaded local values that the backend holds,
// oldest queued first, keeping the blocks read since the last pass. A storage
// move holds passes off through the gate. Returns a zero result without a
// storage backend.
func (p *placementState) evict(ctx context.Context, maxBytes int64) (block_store_writeback.EvictResult, error) {
	// Wait out a storage move.
	var result block_store_writeback.EvictResult
	release, err := p.evictGate.Lock(ctx)
	if err != nil {
		return result, err
	}
	defer release()

	// Evict to the open backend, paced by the account's admit.
	remote := p.remote.Load()
	if remote == nil {
		return result, nil
	}
	admit := func(ctx context.Context, lastBytes int64) error {
		if p.evictAdmit == nil {
			return nil
		}
		if fn := p.evictAdmit.Load(); fn != nil {
			return (*fn)(ctx, lastBytes)
		}
		return nil
	}
	return p.wb.Evict(ctx, remote.store, maxBytes, p.reads.take(), admit)
}

// SetEvictAdmit paces the eviction passes of the account's placed block
// stores: each pass calls admit before each batch, as EvictAdmit describes. A
// nil admit lets passes run unpaced. Running passes see the change at their
// next batch.
func (a *ProviderAccount) SetEvictAdmit(admit block_store_writeback.EvictAdmit) {
	if admit == nil {
		a.evictAdmit.Store(nil)
		return
	}
	a.evictAdmit.Store(&admit)
}
