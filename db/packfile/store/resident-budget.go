package store

import (
	"container/list"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
)

// residentBudget bounds the resident span bytes and parsed index entries of
// every PackReader sharing it.
//
// PackfileStore owns one budget for all of its readers, so resident memory has
// one limit however many packs are open. Readers charge span bytes as spans
// are inserted and removed, and index entries as an index is published and
// dropped. reclaim evicts the least recently used span across all readers
// until the bytes fit, and reclaimIndexes drops a cold index, chosen by the
// CLOCK ring of loaded indexes, until the entries fit. A hot pack keeps its bytes and index while cold
// packs give theirs up; a dropped index reloads from the index cache on its
// next use.
type residentBudget struct {
	// clock orders span and index use across every reader sharing the budget.
	clock atomic.Uint64
	// limit is the byte limit. Zero or negative disables eviction.
	limit atomic.Int64
	// used is the resident span bytes charged by every reader.
	used atomic.Int64
	// indexLimit is the parsed index entry limit. Zero or negative disables
	// dropping indexes.
	indexLimit atomic.Int64
	// indexUsed is the parsed index entries charged by every reader.
	indexUsed atomic.Int64

	// mtx guards readers, indexes and each reader's indexElem. A reader takes
	// mtx under its own lock; the budget never takes a reader lock under mtx.
	mtx sync.Mutex
	// readers are the readers charging the budget.
	readers map[*PackReader]struct{}
	// indexes is the CLOCK ring of readers holding parsed index entries, the
	// next candidate in front. A use sets the reader's indexRef; the sweep
	// gives a referenced reader a second chance at the back of the ring.
	indexes list.List
}

// newResidentBudget builds a budget with the given byte limit.
func newResidentBudget(limit int64) *residentBudget {
	b := &residentBudget{readers: make(map[*PackReader]struct{})}
	b.limit.Store(limit)
	b.indexLimit.Store(defaultIndexEntryBudget)
	return b
}

// overage returns the bytes charged beyond the limit, or zero.
func (b *residentBudget) overage() int64 {
	limit := b.limit.Load()
	if limit <= 0 {
		return 0
	}
	return max(b.used.Load()-limit, 0)
}

// attach registers a reader as an eviction source. The caller holds the
// reader lock.
func (b *residentBudget) attach(e *PackReader) {
	b.mtx.Lock()
	b.readers[e] = struct{}{}
	if len(e.entriesByKey) != 0 {
		b.ringIndexLocked(e)
	}
	b.mtx.Unlock()
}

// detach unregisters a reader. The caller holds the reader lock.
func (b *residentBudget) detach(e *PackReader) {
	b.mtx.Lock()
	delete(b.readers, e)
	b.unringIndexLocked(e)
	b.mtx.Unlock()
}

// indexChanged keeps a reader's place in the index ring in step with its
// parsed entries: a reader holding entries joins the back of the ring as just
// used, and one holding none leaves it. The caller holds the reader lock.
func (b *residentBudget) indexChanged(e *PackReader, entries int) {
	b.mtx.Lock()
	if entries != 0 {
		b.ringIndexLocked(e)
	} else {
		b.unringIndexLocked(e)
	}
	b.mtx.Unlock()
}

// ringIndexLocked puts the reader at the back of the index ring as just used.
func (b *residentBudget) ringIndexLocked(e *PackReader) {
	e.indexRef.Store(true)
	if e.indexElem != nil {
		b.indexes.MoveToBack(e.indexElem)
		return
	}
	e.indexElem = b.indexes.PushBack(e)
}

// unringIndexLocked removes the reader from the index ring.
func (b *residentBudget) unringIndexLocked(e *PackReader) {
	if e.indexElem != nil {
		b.indexes.Remove(e.indexElem)
		e.indexElem = nil
	}
}

// nextIndexVictimLocked sweeps the index ring and returns the first reader
// not used since the sweep last passed it, or nil when the ring holds fewer
// than two indexes. The newest index joins the back as used, so the sweep
// reaches every other index first.
func (b *residentBudget) nextIndexVictimLocked() *PackReader {
	n := b.indexes.Len()
	if n < 2 {
		return nil
	}
	// Two passes clear every reference unless uses keep setting them; then
	// the front is the coldest choice the sweep found.
	for range 2 * n {
		front := b.indexes.Front()
		e := front.Value.(*PackReader)
		if !e.indexRef.Swap(false) {
			return e
		}
		b.indexes.MoveToBack(front)
	}
	return b.indexes.Front().Value.(*PackReader)
}

// reclaim evicts until the budget fits or nothing more can be released.
//
// The caller must not hold any reader lock: reclaim takes each reader's lock
// in turn, never two at once.
func (b *residentBudget) reclaim() {
	for b.overage() > 0 {
		b.mtx.Lock()
		readers := slices.Collect(maps.Keys(b.readers))
		b.mtx.Unlock()

		var victim *PackReader
		var victimSeq uint64
		for _, e := range readers {
			seq, ok := e.oldestEvictableSeq()
			if ok && (victim == nil || seq < victimSeq) {
				victim, victimSeq = e, seq
			}
		}
		if victim == nil {
			return
		}
		victim.evictOldestSpan()
	}
}

// reclaimIndexes drops cold parsed indexes until the entries fit. It never
// drops the last index in the ring, so a reader that just loaded its index can
// use it even when that one index exceeds the limit. Each drop costs a short
// sweep of the ring rather than a visit to every reader.
//
// The caller must not hold any reader lock.
func (b *residentBudget) reclaimIndexes() {
	for {
		limit := b.indexLimit.Load()
		if limit <= 0 || b.indexUsed.Load() <= limit {
			return
		}
		b.mtx.Lock()
		victim := b.nextIndexVictimLocked()
		b.mtx.Unlock()
		if victim == nil {
			return
		}
		victim.dropIndex()
	}
}
