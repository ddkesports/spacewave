package store

import (
	"context"

	"github.com/s4wave/spacewave/db/block/bloom"
	"github.com/s4wave/spacewave/db/packfile"
)

// Locator places a block key in a pack without the bloom filters.
//
// The store consults it before the filters: a pack the locator Indexed is
// found through Locate alone, and only the packs it has not indexed still
// test their bloom filter. Once every pack of the manifest is indexed, a key
// Locate does not place is in no pack, so the lookup ends without opening one.
type Locator interface {
	// Locate returns the id of the pack that holds key, and false when no
	// indexed pack does. The pack may have left the manifest since.
	Locate(ctx context.Context, key []byte) (packID string, found bool, err error)
	// Indexed reports whether Locate knows every key of the pack.
	Indexed(packID string) bool
	// Version changes whenever Indexed may answer differently for some pack.
	Version() uint64
}

// SetLocator sets the locator the lookups consult. Call it before the first
// lookup.
func (s *PackfileStore) SetLocator(l Locator) {
	s.locator = l
}

// unindexedPacks is a snapshot's catalog split by the locator at one version.
type unindexedPacks struct {
	// version is the locator version the split was computed at.
	version uint64
	// open holds the packs the locator has not indexed, in lookup order.
	open []*manifestEntry
}

// unindexedPacks returns the snapshot's packs the locator has not indexed,
// computing the split once per snapshot and locator version; concurrent
// lookups wait for the one computing it. The version is read before the
// split, so a change during it makes the next lookup recompute.
func (v *ManifestSnapshot) unindexedPacks(loc Locator) *unindexedPacks {
	// Reuse the split while the locator answers as it did.
	version := loc.Version()
	if packs := v.unindexed.Load(); packs != nil && packs.version == version {
		return packs
	}
	v.unindexedMtx.Lock()
	defer v.unindexedMtx.Unlock()
	if packs := v.unindexed.Load(); packs != nil && packs.version >= version {
		return packs
	}

	// Collect the packs the locator has not indexed.
	packs := &unindexedPacks{version: version}
	v.entries.Scan(func(item *manifestEntry) bool {
		if !loc.Indexed(item.entry.GetId()) {
			packs.open = append(packs.open, item)
		}
		return true
	})
	v.unindexed.Store(packs)
	return packs
}

// entryByID returns the snapshot's entry for pack id, or nil.
func (v *ManifestSnapshot) entryByID(id string) *manifestEntry {
	item, _ := v.ids.Get(&manifestEntry{entry: &packfile.PackfileEntry{Id: id}})
	return item
}

// candidates returns the packs of view that may hold key, the pack the locator
// places it in first.
func (s *PackfileStore) candidates(ctx context.Context, view *ManifestSnapshot, key []byte) ([]*manifestEntry, error) {
	// Without a locator every pack tests its filter.
	bloomKey := bloom.NewKey(key)
	loc := s.locator
	if loc == nil {
		return view.filterCandidates(bloomKey), nil
	}

	// Ask the locator which pack holds the key.
	located, found, err := loc.Locate(ctx, key)
	if err != nil {
		return nil, err
	}

	// Place the located pack first. When it left the manifest, the index
	// cannot rule out the packs it covers: test every filter.
	packs := view.unindexedPacks(loc)
	var out []*manifestEntry
	if found {
		hit := view.entryByID(located)
		if hit == nil {
			return view.filterCandidates(bloomKey), nil
		}
		out = append(out, hit)
	}

	// Test the filters of the packs the locator has not indexed.
	for _, item := range packs.open {
		if found && item == out[0] {
			continue
		}
		if item.filter == nil || item.filter.TestKey(bloomKey) {
			out = append(out, item)
		}
	}
	return out, nil
}

// filterCandidates returns every pack of view whose filter may hold key.
func (v *ManifestSnapshot) filterCandidates(key bloom.Key) []*manifestEntry {
	var out []*manifestEntry
	v.entries.Scan(func(item *manifestEntry) bool {
		if item.filter == nil || item.filter.TestKey(key) {
			out = append(out, item)
		}
		return true
	})
	return out
}
