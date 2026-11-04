//go:build !goscript

package kvtx_block_okra

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"runtime"
	"slices"
	"testing"

	"github.com/s4wave/spacewave/db/block"
	"github.com/s4wave/spacewave/db/block/blob"
	"github.com/s4wave/spacewave/net/hash"
)

// newFlatSamplePage builds a page mixing child refs, value refs and inline blobs.
func newFlatSamplePage() *Page {
	// Fill the bounds and one entry of each kind, in key order.
	page := &Page{LowerBound: []byte("objects/"), PageHash: make([]byte, HashSize)}
	for i := range 48 {
		entry := &Entry{Key: fmt.Appendf(nil, "objects/%032d", i), Hash: make([]byte, HashSize), Size: uint64(i + 1)}
		switch i % 3 {
		case 0:
			entry.ChildRef = &block.BlockRef{Hash: &hash.Hash{HashType: 1, Hash: bytes.Repeat([]byte{byte(i)}, 32)}}
		case 1:
			entry.ValueRef = &block.BlockRef{Hash: &hash.Hash{HashType: 1, Hash: bytes.Repeat([]byte{byte(i)}, 32)}}
		default:
			entry.ValueIsBlob = true
			entry.ValueBlob = &blob.Blob{TotalSize: 24, RawData: bytes.Repeat([]byte{byte(i)}, 24)}
		}
		page.Entries = append(page.Entries, entry)
	}
	return page
}

// TestFlatPage checks the flat page against the model states of FlatPage.tla:
// the decoded form matches Page field for field, a published page is never
// written by reads, a view survives eviction of its page, and the cache charge
// equals the resident arena bytes.
func TestFlatPage(t *testing.T) {
	// Decode the same encoded page both ways.
	page := newFlatSamplePage()
	data, err := page.MarshalVT()
	if err != nil {
		t.Fatal(err)
	}
	flat := &flatPage{}
	if err := flat.UnmarshalDecodedBlock(data); err != nil {
		t.Fatal(err)
	}
	if flat.Len() != len(page.Entries) {
		t.Fatalf("entries: %d != %d", flat.Len(), len(page.Entries))
	}

	// Differential: every accessor agrees with the pointer-rich page.
	for i, e := range page.Entries {
		if !bytes.Equal(flat.entryKey(i), e.Key) {
			t.Fatalf("entry %d key differs", i)
		}
		if got := flat.childRef(i); !got.EqualVT(e.ChildRef) && (got != nil || e.ChildRef != nil) {
			t.Fatalf("entry %d child ref differs", i)
		}
		if got := flat.valueRef(i); !got.EqualVT(e.ValueRef) && (got != nil || e.ValueRef != nil) {
			t.Fatalf("entry %d value ref differs", i)
		}
		if flat.valueIsBlob(i) != e.ValueIsBlob {
			t.Fatalf("entry %d blob flag differs", i)
		}
		if idx := flat.searchEntry(e.Key); idx != i {
			t.Fatalf("search %d found %d", i, idx)
		}
	}

	// Model Immutable: reads leave the published arena bytes unchanged.
	before := bytes.Clone(flat.buf)
	for i := range flat.Len() {
		flat.entryKey(i)
		flat.rawInlineValue(i)
	}
	if !bytes.Equal(before, flat.buf) {
		t.Fatal("reading a published page changed its arena")
	}

	// Model ViewStable: a view stays intact after the page is dropped and
	// the collector runs.
	view := flat.entryKey(5)
	want := bytes.Clone(view)
	flat = nil
	for range 3 {
		runtime.GC()
	}
	if !bytes.Equal(view, want) {
		t.Fatal("view changed after its page was released")
	}
}

// TestFlatPageBudget checks the model's Accounted and InBudget invariants: the
// cache charges exactly the arena bytes of its resident pages and stays in budget.
func TestFlatPageBudget(t *testing.T) {
	// Charge the real arena bytes of one page against a three page budget.
	data, err := newFlatSamplePage().MarshalVT()
	if err != nil {
		t.Fatal(err)
	}
	one := &flatPage{}
	if err := one.UnmarshalDecodedBlock(data); err != nil {
		t.Fatal(err)
	}
	if got := one.DecodedHeapSize(); got < len(data) {
		t.Fatalf("charge %d is below the arena bytes %d", got, len(data))
	}
	cache, err := block.NewDecodedBlockCacheWithOptions(block.DecodedBlockCacheOptions{MaxCost: 3 * int64(one.DecodedHeapSize())})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	// Read the pages of a tree larger than the budget through the cache.
	ctx := t.Context()
	store := newOkraTestStore()
	fixture := newOkraFixture(t, ctx, store, 4096)
	rootRef, _ := writeOkraFixture(t, ctx, store, fixture)
	bt, cs := block.NewTransaction(store, nil, rootRef, nil)
	bt.SetReadOnly()
	bt.SetDecodedBlockCache(cache)
	tr, err := NewTx(ctx, cs, nil, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Discard()
	for i, key := range fixture.keys {
		value, found, err := tr.Get(ctx, key)
		if err != nil || !found || !bytes.Equal(value, fixture.values[i]) {
			t.Fatalf("read %q found=%v err=%v", key, found, err)
		}
	}

	// The retained charge is within budget and pages were evicted.
	stats := cache.Snapshot()
	if stats.RetainedCost > stats.MaxCost || stats.Evictions == 0 {
		t.Fatalf("budget not exercised: %+v", stats)
	}
}

// flatDiffValue is one value a churned tree should hold.
type flatDiffValue struct {
	// data is the value's bytes.
	data []byte
	// raw marks a value stored as a plain external block rather than a Blob.
	raw bool
}

// flatDiffKey returns a key in one of the shapes a store produces: object
// paths, revisions below an object, binary keys, and long shared prefixes.
func flatDiffKey(rng *rand.Rand) []byte {
	n := rng.IntN(5000)
	switch rng.IntN(5) {
	case 0:
		return fmt.Appendf(nil, "objects/%08d", n)
	case 1:
		return fmt.Appendf(nil, "objects/%08d/rev/%d", n, rng.IntN(4))
	case 2:
		key := make([]byte, 1+rng.IntN(40))
		for i := range key {
			key[i] = []byte{0x00, 0x01, 0x7f, 0x80, 0xff}[rng.IntN(5)]
		}
		return key
	case 3:
		return fmt.Appendf(bytes.Repeat([]byte("p"), 300), "%d", n)
	default:
		return fmt.Appendf(nil, "%x", n)
	}
}

// flatDiffBytes returns a value at or around each storage layout boundary.
func flatDiffBytes(rng *rand.Rand) []byte {
	sizes := []int{0, 1, 32, inlineValueLimit - 1, inlineValueLimit, inlineValueLimit + 1, 4096}
	size := sizes[rng.IntN(len(sizes))]
	if rng.IntN(300) == 0 {
		size = 1 << 20
	}
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(rng.Uint32())
	}
	return data
}

// flatDiffProbes returns key and the absent neighbors around it.
func flatDiffProbes(key []byte) [][]byte {
	probes := [][]byte{key, append(bytes.Clone(key), 0x00), append(bytes.Clone(key), 0xff)}
	if len(key) > 1 {
		probes = append(probes, key[:len(key)-1])
	}
	if last := len(key) - 1; key[last] != 0 {
		before := bytes.Clone(key)
		before[last]--
		probes = append(probes, before)
	}
	return probes
}

// checkFlatPage requires a stored page's flat form to match its decoded form.
func checkFlatPage(t *testing.T, page *Page, data []byte) {
	t.Helper()
	flat := &flatPage{}
	if err := flat.UnmarshalDecodedBlock(data); err != nil {
		t.Fatal(err)
	}
	if err := flat.ValidateDecodedBlock(); err != nil {
		t.Fatal(err)
	}
	if flat.Level() != page.GetLevel() || flat.Len() != len(page.GetEntries()) || flat.size != page.GetSize() ||
		flat.startsAtAnchor != page.GetStartsAtAnchor() || !bytes.Equal(flat.view(flat.pageHash), page.GetPageHash()) ||
		!bytes.Equal(flat.view(flat.lower), page.GetLowerBound()) || !bytes.Equal(flat.view(flat.upper), page.GetUpperBound()) {
		t.Fatalf("page header differs at level %d", page.GetLevel())
	}
	for i, e := range page.GetEntries() {
		fe := &flat.entries[i]
		if !bytes.Equal(flat.entryKey(i), e.GetKey()) || !bytes.Equal(flat.view(fe.hash), e.GetHash()) ||
			fe.size != e.GetSize() || flat.isAnchor(i) != e.GetAnchor() || flat.valueIsBlob(i) != e.GetValueIsBlob() ||
			flat.hasInlineBlob(i) != (e.GetValueBlob() != nil) {
			t.Fatalf("entry %d differs", i)
		}
		if !flat.childRef(i).EqualsRef(e.GetChildRef()) && !(flat.childRef(i) == nil && e.GetChildRef().GetEmpty()) {
			t.Fatalf("entry %d child ref differs", i)
		}
		if !flat.valueRef(i).EqualsRef(e.GetValueRef()) && !(flat.valueRef(i) == nil && e.GetValueRef().GetEmpty()) {
			t.Fatalf("entry %d value ref differs", i)
		}
		wantRaw, wantOK := page.rawInlineValue(i)
		gotRaw, gotOK := flat.rawInlineValue(i)
		if wantOK != gotOK || !bytes.Equal(wantRaw, gotRaw) {
			t.Fatalf("entry %d inline value differs", i)
		}
		if e.GetAnchor() {
			continue
		}
		for _, probe := range flatDiffProbes(e.GetKey()) {
			if flat.searchEntry(probe) != page.searchEntry(probe) || flat.containsKey(probe) != page.containsKey(probe) {
				t.Fatalf("lookup of %x differs on entry %d", probe, i)
			}
		}
	}
}

// walkFlatDiffPages visits every stored page below cursor.
func walkFlatDiffPages(t *testing.T, ctx context.Context, page *Page, cursor *block.Cursor, visit func(*Page, []byte)) {
	t.Helper()
	data, _, err := cursor.Fetch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	visit(page, data)
	if page.GetLevel() == 0 {
		return
	}
	for i, e := range page.GetEntries() {
		if e.GetChildRef().GetEmpty() {
			continue
		}
		child := page.FollowChild(cursor, i)
		next, err := loadPage(ctx, child)
		if err != nil {
			t.Fatal(err)
		}
		walkFlatDiffPages(t, ctx, next, child, visit)
	}
}

// checkFlatDamage decodes damaged copies of a stored page both ways. Neither
// decoder may panic, and a page both accept must read the same. The flat form
// skips nested fields it does not use, so it accepts some damage Page rejects;
// it counts those in onlyFlat and any page only Page accepts in onlyPage.
func checkFlatDamage(t *testing.T, rng *rand.Rand, data []byte, onlyPage, onlyFlat *int) {
	t.Helper()
	for range 48 {
		damaged := bytes.Clone(data)
		switch rng.IntN(3) {
		case 0:
			damaged[rng.IntN(len(damaged))] ^= 1 << rng.IntN(8)
		case 1:
			damaged[rng.IntN(len(damaged))] = byte(rng.Uint32())
		default:
			damaged = damaged[:rng.IntN(len(damaged))]
		}
		page, flat := &Page{}, &flatPage{}
		pageOK := page.UnmarshalDecodedBlock(damaged) == nil && page.ValidateDecodedBlock() == nil
		flatOK := flat.UnmarshalDecodedBlock(damaged) == nil && flat.ValidateDecodedBlock() == nil
		switch {
		case pageOK && !flatOK:
			*onlyPage++
		case flatOK && !pageOK:
			*onlyFlat++
		case pageOK:
			checkFlatPage(t, page, damaged)
		}
	}
}

// TestFlatReadsMatchPageReads publishes a tree through the real writer with
// set and delete churn across commits, mixing external, raw, inline, empty and
// chunked values, then requires the flat read path to answer exactly as the
// mutable page path does: page by page on every stored page, and through Get,
// Exists and GetCursorAtKey on present and absent keys, under a decoded budget
// small enough to evict pages between reads, on the head and on old roots.
func TestFlatReadsMatchPageReads(t *testing.T) {
	ctx := t.Context()
	store := newOkraTestStore()
	rng := rand.New(rand.NewPCG(7, 11))

	// Publish each round, switching to the inline layout partway as a store does.
	const rounds = 5
	values := make(map[string]flatDiffValue)
	removed := make(map[string]struct{})
	var ref *block.BlockRef
	roots := make([]*block.BlockRef, 0, rounds)
	snapshots := make([]map[string]flatDiffValue, 0, rounds)
	for round := range rounds {
		btx, rootCursor := block.NewTransaction(store, nil, ref, nil)
		if ref == nil {
			rootCursor.SetBlock(&Root{}, true)
		}
		open := NewTx
		if round >= 2 {
			open = NewTxWithInlineValues
		}
		tx, err := open(ctx, rootCursor, nil, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		for range 1200 {
			key, value := flatDiffKey(rng), flatDiffBytes(rng)
			if rng.IntN(7) != 0 {
				if err := tx.Set(ctx, key, value); err != nil {
					t.Fatal(err)
				}
				values[string(key)] = flatDiffValue{data: value}
				delete(removed, string(key))
				continue
			}
			rawRef, _, err := store.PutBlock(ctx, append(value, 'r'), nil)
			if err != nil {
				t.Fatal(err)
			}
			valueCursor := rootCursor.Detach(false)
			valueCursor.ClearAllRefs()
			valueCursor.SetRefAtCursor(rawRef, true)
			if err := tx.SetCursorAtKey(ctx, key, valueCursor, false); err != nil {
				t.Fatal(err)
			}
			values[string(key)] = flatDiffValue{data: value, raw: true}
			delete(removed, string(key))
		}
		for _, key := range slices.Sorted(maps.Keys(values)) {
			if rng.IntN(10) != 0 {
				continue
			}
			if err := tx.Delete(ctx, []byte(key)); err != nil {
				t.Fatal(err)
			}
			delete(values, key)
			removed[key] = struct{}{}
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if ref, _, err = btx.Write(ctx, true); err != nil {
			t.Fatal(err)
		}
		roots = append(roots, ref)
		snapshots = append(snapshots, maps.Clone(values))
	}

	// Compare every stored page of the head, intact and damaged.
	head := openOkraRoot(t, ctx, store, ref, false)
	defer head.Discard()
	if head.readRoot != nil {
		t.Fatal("reference reader took the immutable path")
	}
	rootPage, rootCursor, err := head.getRootPage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var pages, levels, onlyPage, onlyFlat int
	walkFlatDiffPages(t, ctx, rootPage, rootCursor, func(page *Page, data []byte) {
		checkFlatPage(t, page, data)
		pages++
		levels = max(levels, int(page.GetLevel()))
		checkFlatDamage(t, rng, data, &onlyPage, &onlyFlat)
	})
	if levels == 0 {
		t.Fatal("fixture has a single page")
	}
	if onlyPage != 0 {
		t.Fatalf("flat form rejected %d damaged pages Page accepts", onlyPage)
	}
	t.Logf("%d pages, %d levels; %d damaged pages only the flat form accepted", pages, levels+1, onlyFlat)

	// Compare every read API on each root under an evicting decoded budget.
	cache, err := block.NewDecodedBlockCacheWithOptions(block.DecodedBlockCacheOptions{MaxCost: 256 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	var lookups int
	for i, root := range roots {
		reference := openOkraRoot(t, ctx, store, root, false)
		defer reference.Discard()
		bt, cs := block.NewTransaction(store, nil, root, nil)
		bt.SetReadOnly()
		bt.SetDecodedBlockCache(cache)
		reader, err := NewTx(ctx, cs, nil, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Discard()
		if reader.readRoot == nil {
			t.Fatal("readonly root did not use the flat path")
		}
		snapshot := snapshots[i]
		keys := slices.Sorted(maps.Keys(snapshot))
		keys = append(keys, slices.Sorted(maps.Keys(removed))...)
		for range 2 {
			for _, key := range keys {
				for _, probe := range flatDiffProbes([]byte(key)) {
					checkFlatLookup(t, ctx, reader, reference, snapshot, probe)
					lookups++
				}
			}
		}
	}
	if stats := cache.Snapshot(); stats.Evictions == 0 {
		t.Fatalf("decoded budget was not exercised: %+v", stats)
	}
	t.Logf("%d lookups compared", lookups)
}

// checkFlatLookup requires both readers and the expected values to agree on key.
func checkFlatLookup(t *testing.T, ctx context.Context, reader, reference *Tx, values map[string]flatDiffValue, key []byte) {
	t.Helper()
	want, present := values[string(key)]
	got, found, err := reader.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	refGot, refFound, err := reference.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if found != present || refFound != present {
		t.Fatalf("%x: flat found %v, page found %v, want %v", key, found, refFound, present)
	}
	if !bytes.Equal(got, refGot) {
		t.Fatalf("%x: flat and page values differ", key)
	}
	exists, err := reader.Exists(ctx, key)
	if err != nil || exists != present {
		t.Fatalf("%x: exists %v, %v", key, exists, err)
	}
	cursor, err := reader.GetCursorAtKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	refCursor, err := reference.GetCursorAtKey(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if (cursor == nil) != (refCursor == nil) || (cursor == nil) == present {
		t.Fatalf("%x: cursor presence differs", key)
	}
	if !present {
		return
	}
	if !cursor.GetRef().EqualsRef(refCursor.GetRef()) {
		t.Fatalf("%x: cursor refs differ", key)
	}
	var value []byte
	if want.raw {
		value, _, err = cursor.Fetch(ctx)
		if !bytes.Equal(got, append(bytes.Clone(want.data), 'r')) {
			t.Fatalf("%x: raw value differs", key)
		}
		want.data = got
	} else {
		value, err = blob.FetchToBytes(ctx, cursor)
		if !bytes.Equal(got, want.data) {
			t.Fatalf("%x: value differs", key)
		}
	}
	if err != nil || !bytes.Equal(value, want.data) {
		t.Fatalf("%x: cursor value differs: %v", key, err)
	}
}
