package sobject_world_engine

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/s4wave/spacewave/core/bstore"
	"github.com/s4wave/spacewave/core/sobject"
	"github.com/s4wave/spacewave/db/block"
)

// worldPagesMaxBytes bounds the length of the pages a worldPages keeps. A page
// offered past it is left for the caller to store.
const worldPagesMaxBytes = 128 << 20

// worldPages is the SharedObject's block store with the World pages the engine
// computed and has not stored in front of it. Every World reader the engine
// builds reads through it, so a page is readable from the moment it is kept.
//
// Invariants:
//   - A kept page carries its complete child list, so a walk through kept
//     pages never reads the store to find a child.
//   - A page leaves only once the store holds it durably (stored) or no live
//     root and no reader pin reaches it (sweep). A reader that finds a page
//     missing here finds it in the store, or holds nothing that reaches it.
//   - A reader pin on a kept page becomes a pin in the store before the page
//     leaves for the store, so the collector never sees the page unpinned.
//   - No completion proof names a kept root: its graph is not stored.
type worldPages struct {
	// pageView serves kept pages and forwards the rest to the store.
	pageView
	// inner is the SharedObject's block store.
	inner bstore.BlockStore
}

// newWorldPages wraps the SharedObject's block store with an empty page set.
func newWorldPages(inner bstore.BlockStore) *worldPages {
	set := &pageSet{pages: make(map[string]*keptPage)}
	return &worldPages{pageView: pageView{StoreOps: inner, set: set}, inner: inner}
}

// newWorldPages returns the page view of so's block store, or nil when the
// engine stores every page as it computes it: when the Config asks it to, and
// for an object whose publications copy the World into another store, which
// reads the pages from so's block store.
func (c *Controller) newWorldPages(so sobject.SharedObject) *worldPages {
	if c.conf.GetStoreEveryWorld() {
		return nil
	}
	if _, ok := so.(sobject.PublicationRetention); ok {
		return nil
	}
	return newWorldPages(so.GetBlockStore())
}

// keep adds the World pages among entries and returns the entries it did not
// keep, which the caller stores: tombstones, blocks without a complete child
// list, and pages past the length bound.
func (w *worldPages) keep(entries []*block.PutBatchEntry) []*block.PutBatchEntry {
	return w.set.keep(entries)
}

// stored drops the kept pages the roots reach once a durable write put them in
// the store, moving each reader pin on them to the store first. On failure
// the pages stay kept.
func (w *worldPages) stored(ctx context.Context, roots []*block.BlockRef) error {
	// Take the pages and the pins on them.
	w.set.mu.Lock()
	taken := make(map[string]*keptPage)
	var pins []*pagePin
	var pinned []*block.BlockRef
	for key := range w.set.reach(roots) {
		page := w.set.pages[key]
		taken[key] = page
		for _, pin := range page.pins {
			pins, pinned = append(pins, pin), append(pinned, page.entry.Ref)
		}
		w.set.remove(key)
	}
	w.set.mu.Unlock()

	// Pin each pinned page in the store, which now holds it. A failure puts
	// the pages back with the pins not yet moved, so no pin is ever left
	// uncovered.
	for i, pin := range pins {
		release, err := block.PinRoot(ctx, w.inner, pinned[i])
		if err != nil {
			w.set.restore(taken)
			return err
		}
		w.set.mu.Lock()
		released := pin.released
		if !released {
			pin.release = release
		}
		w.set.mu.Unlock()
		if released {
			release()
		}
	}
	return nil
}

// sweep drops the kept pages that neither a live root nor a reader pin reaches.
func (w *worldPages) sweep(live []*block.BlockRef) {
	// Mark from the live roots and the pinned pages, then drop the rest.
	w.set.mu.Lock()
	defer w.set.mu.Unlock()
	roots := slices.Clone(live)
	for _, page := range w.set.pages {
		if len(page.pins) != 0 {
			roots = append(roots, page.entry.Ref)
		}
	}
	reached := w.set.reach(roots)
	for key := range w.set.pages {
		if _, ok := reached[key]; !ok {
			w.set.remove(key)
		}
	}
}

// keptBytes returns the length of the kept pages.
func (w *worldPages) keptBytes() int {
	w.set.mu.RLock()
	defer w.set.mu.RUnlock()
	return w.set.bytes
}

// GetID returns the store's ID.
func (w *worldPages) GetID() string {
	return w.inner.GetID()
}

// GetDecodedBlockCache returns the store's decoded block cache.
func (w *worldPages) GetDecodedBlockCache() *block.DecodedBlockCache {
	return w.inner.GetDecodedBlockCache()
}

// ReclaimStorage forwards the backend reclaim pass. Kept pages are not in the
// backend, so the pass cannot drop them.
func (w *worldPages) ReclaimStorage(ctx context.Context, fence bstore.ReclaimFence) (time.Time, error) {
	return w.inner.ReclaimStorage(ctx, fence)
}

// AtomicPublicationVolumeID returns the store's publication namespace.
func (w *worldPages) AtomicPublicationVolumeID() string {
	if !w.SupportsAtomicPublication() {
		return ""
	}
	return w.inner.(block.AtomicPublisher).AtomicPublicationVolumeID()
}

// SupportsAtomicPublication reports whether the store publishes atomically.
func (w *worldPages) SupportsAtomicPublication() bool {
	p, ok := w.inner.(block.AtomicPublisher)
	return ok && p.SupportsAtomicPublication()
}

// SubmitAtomic forwards a publication to the store.
func (w *worldPages) SubmitAtomic(ctx context.Context, p *block.AtomicPublication) (*block.PublicationReceipt, error) {
	if !w.SupportsAtomicPublication() {
		return nil, block.ErrAtomicPublicationUnsupported
	}
	return w.inner.(block.AtomicPublisher).SubmitAtomic(ctx, p)
}

// PublishAtomic forwards a publication to the store and waits for it.
func (w *worldPages) PublishAtomic(ctx context.Context, p *block.AtomicPublication) error {
	if !w.SupportsAtomicPublication() {
		return block.ErrAtomicPublicationUnsupported
	}
	return w.inner.(block.AtomicPublisher).PublishAtomic(ctx, p)
}

// SupportsRootRetention reports the store's root ownership.
func (w *worldPages) SupportsRootRetention() bool {
	return block.SupportsRootRetention(w.inner)
}

// SetRetainedRoots forwards named roots to the store.
func (w *worldPages) SetRetainedRoots(ctx context.Context, entries []*block.PutBatchEntry, roots []block.NamedRoot) error {
	return block.SetRetainedRoots(ctx, w.inner, entries, roots)
}

// PinRoot protects ref until release. A kept page is not in the store, so no
// sweep there can collect it: the pin waits here and moves to the store when
// stored drops the page.
func (w *worldPages) PinRoot(ctx context.Context, ref *block.BlockRef) (func(), error) {
	// Pin a kept page here.
	key := ref.MarshalString()
	pin := &pagePin{}
	w.set.mu.Lock()
	page := w.set.pages[key]
	if page != nil {
		page.pins = append(page.pins, pin)
	}
	w.set.mu.Unlock()
	if page != nil {
		return sync.OnceFunc(func() { w.set.unpin(key, pin) }), nil
	}

	// Pin a root the store holds.
	return block.PinRoot(ctx, w.inner, ref)
}

// OpenStage opens a stage on the store. Reads see the kept pages; writes go
// to the stage.
func (w *worldPages) OpenStage(ctx context.Context) (block.StoreOps, func(), error) {
	stage, release, err := block.OpenStage(ctx, w.inner)
	if err != nil {
		return nil, nil, err
	}
	return block.NewStoreRW(&pageView{StoreOps: w.inner, set: w.set}, stage), release, nil
}

// ReleaseRoots drops the staging ownership of roots in the store.
func (w *worldPages) ReleaseRoots(ctx context.Context, refs []*block.BlockRef) error {
	return block.ReleaseRoots(ctx, w.inner, refs)
}

// MarkRootsComplete records the completion proofs of the roots the store
// holds. A kept root's graph is not stored, so it gets no proof.
func (w *worldPages) MarkRootsComplete(ctx context.Context, roots []*block.BlockRef) error {
	return block.MarkRootsComplete(ctx, w.inner, w.set.unkept(roots))
}

// RootsComplete reports the completion proof of each root. A kept root has
// none.
func (w *worldPages) RootsComplete(ctx context.Context, refs []*block.BlockRef) ([]bool, error) {
	// Ask the store, then clear the proof of each kept root.
	complete, err := block.RootsComplete(ctx, w.inner, refs)
	if err != nil {
		return nil, err
	}
	w.set.mu.RLock()
	defer w.set.mu.RUnlock()
	for i, ref := range refs {
		if w.set.pages[ref.MarshalString()] != nil {
			complete[i] = false
		}
	}
	return complete, nil
}

// BeginDeferFlush forwards the GC defer-flush scope to the store.
func (w *worldPages) BeginDeferFlush() {
	block.BeginDeferFlush(w.inner)
}

// EndDeferFlush closes the GC defer-flush scope on the store.
func (w *worldPages) EndDeferFlush(ctx context.Context) error {
	return block.EndDeferFlush(ctx, w.inner)
}

// pageView serves the kept pages of a pageSet in front of a store.
type pageView struct {
	// StoreOps is the store behind the kept pages.
	block.StoreOps
	// set holds the kept pages.
	set *pageSet
}

// BeginReadOperation opens a read scope on the store that also sees the kept
// pages.
func (v *pageView) BeginReadOperation(ctx context.Context) (block.StoreOps, func(), error) {
	ops, release, err := v.StoreOps.BeginReadOperation(ctx)
	if err != nil {
		return nil, nil, err
	}
	return &pageView{StoreOps: ops, set: v.set}, release, nil
}

// EnsureDecodedBlockCacheFresh forwards decoded-cache freshness to the store.
func (v *pageView) EnsureDecodedBlockCacheFresh(ctx context.Context) error {
	freshener, ok := v.StoreOps.(block.DecodedBlockCacheFreshener)
	if !ok {
		return nil
	}
	return freshener.EnsureDecodedBlockCacheFresh(ctx)
}

// GetBlock returns a kept page or reads the store.
func (v *pageView) GetBlock(ctx context.Context, ref *block.BlockRef) ([]byte, bool, error) {
	if e := v.set.get(ref); e != nil {
		return e.Data, true, nil
	}
	return v.StoreOps.GetBlock(ctx, ref)
}

// GetStoredBlock returns a kept page with its child list or reads the store.
func (v *pageView) GetStoredBlock(ctx context.Context, ref *block.BlockRef) (*block.StoredBlock, error) {
	if e := v.set.get(ref); e != nil {
		return &block.StoredBlock{Data: e.Data, Refs: e.Refs, RefsKnown: true}, nil
	}
	return v.StoreOps.GetStoredBlock(ctx, ref)
}

// GetBlockExists reports a kept page or asks the store.
func (v *pageView) GetBlockExists(ctx context.Context, ref *block.BlockRef) (bool, error) {
	if v.set.get(ref) != nil {
		return true, nil
	}
	return v.StoreOps.GetBlockExists(ctx, ref)
}

// GetBlockExistsBatch reports the kept pages and asks the store for the rest.
func (v *pageView) GetBlockExistsBatch(ctx context.Context, refs []*block.BlockRef) ([]bool, error) {
	// Answer the kept pages.
	out := make([]bool, len(refs))
	var rest []*block.BlockRef
	var restIdx []int
	for i, ref := range refs {
		if v.set.get(ref) != nil {
			out[i] = true
			continue
		}
		rest = append(rest, ref)
		restIdx = append(restIdx, i)
	}
	if len(rest) == 0 {
		return out, nil
	}

	// Ask the store for the rest.
	found, err := v.StoreOps.GetBlockExistsBatch(ctx, rest)
	if err != nil {
		return nil, err
	}
	for i, ok := range found {
		out[restIdx[i]] = ok
	}
	return out, nil
}

// StatBlock reports a kept page or asks the store.
func (v *pageView) StatBlock(ctx context.Context, ref *block.BlockRef) (*block.BlockStat, error) {
	if e := v.set.get(ref); e != nil {
		return &block.BlockStat{Ref: ref.Clone(), Size: int64(len(e.Data))}, nil
	}
	return v.StoreOps.StatBlock(ctx, ref)
}

// pageSet holds kept World pages by reference.
type pageSet struct {
	// mu guards the fields below.
	mu sync.RWMutex
	// pages are the kept pages by marshaled reference.
	pages map[string]*keptPage
	// bytes is the length of the kept pages.
	bytes int
}

// keptPage is a World page and the reader pins on it.
type keptPage struct {
	// entry is the page with its complete child list.
	entry *block.PutBatchEntry
	// pins are the reader pins taken while the page was kept.
	pins []*pagePin
}

// pagePin is a reader pin taken on a kept page.
type pagePin struct {
	// release drops the store pin, or is nil while the page is kept.
	release func()
	// released records that the holder released the pin before it moved to
	// the store.
	released bool
}

// keep adds the World pages among entries and returns the rest.
func (s *pageSet) keep(entries []*block.PutBatchEntry) []*block.PutBatchEntry {
	// Add each page within the bound once.
	s.mu.Lock()
	defer s.mu.Unlock()
	var rest []*block.PutBatchEntry
	for _, e := range entries {
		if e.Tombstone || !e.RefsKnown || s.bytes+len(e.Data) > worldPagesMaxBytes {
			rest = append(rest, e)
			continue
		}
		key := e.Ref.MarshalString()
		if s.pages[key] != nil {
			continue
		}
		s.pages[key] = &keptPage{entry: e}
		s.bytes += len(e.Data)
	}
	return rest
}

// get returns the kept page ref names, or nil.
func (s *pageSet) get(ref *block.BlockRef) *block.PutBatchEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if page := s.pages[ref.MarshalString()]; page != nil {
		return page.entry
	}
	return nil
}

// reach returns the keys of the kept pages the roots reach through kept
// pages. Caller holds mu.
func (s *pageSet) reach(roots []*block.BlockRef) map[string]struct{} {
	reached := make(map[string]struct{})
	stack := slices.Clone(roots)
	for len(stack) != 0 {
		ref := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if ref.GetEmpty() {
			continue
		}
		key := ref.MarshalString()
		page := s.pages[key]
		if page == nil {
			continue
		}
		if _, ok := reached[key]; ok {
			continue
		}
		reached[key] = struct{}{}
		stack = append(stack, page.entry.Refs...)
	}
	return reached
}

// remove drops the kept page under key. Caller holds mu.
func (s *pageSet) remove(key string) {
	s.bytes -= len(s.pages[key].entry.Data)
	delete(s.pages, key)
}

// restore puts taken pages back with the pins still waiting on them, except a
// page kept again since, whose waiting pins join the kept copy.
func (s *pageSet) restore(taken map[string]*keptPage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, page := range taken {
		page.pins = slices.DeleteFunc(page.pins, func(p *pagePin) bool { return p.release != nil || p.released })
		if cur := s.pages[key]; cur != nil {
			cur.pins = append(cur.pins, page.pins...)
			continue
		}
		s.pages[key] = page
		s.bytes += len(page.entry.Data)
	}
}

// unpin releases pin on the page under key, the store pin once the page
// moved to the store.
func (s *pageSet) unpin(key string, pin *pagePin) {
	// Drop a waiting pin here, or release the store pin it moved to.
	s.mu.Lock()
	release := pin.release
	if release == nil {
		pin.released = true
		if page := s.pages[key]; page != nil {
			page.pins = slices.DeleteFunc(page.pins, func(p *pagePin) bool { return p == pin })
		}
	}
	s.mu.Unlock()
	if release != nil {
		release()
	}
}

// unkept returns the refs that name no kept page.
func (s *pageSet) unkept(refs []*block.BlockRef) []*block.BlockRef {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.DeleteFunc(slices.Clone(refs), func(ref *block.BlockRef) bool {
		return s.pages[ref.MarshalString()] != nil
	})
}

// _ is a type assertion
var (
	_ bstore.BlockStore                = (*worldPages)(nil)
	_ block.AtomicPublisher            = (*worldPages)(nil)
	_ block.RootRetainer               = (*worldPages)(nil)
	_ block.DeferFlusher               = (*worldPages)(nil)
	_ block.DecodedBlockCacheFreshener = (*pageView)(nil)
)
