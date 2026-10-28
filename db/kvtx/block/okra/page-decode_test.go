package kvtx_block_okra

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"

	"github.com/s4wave/spacewave/db/block"
	"github.com/s4wave/spacewave/db/block/blob"
	"github.com/s4wave/spacewave/net/hash"
)

// decodeTestPage builds a page of n entries at level. Leaf pages after the
// anchor alternate value refs and inline blobs when inline is set.
func decodeTestPage(level uint32, n int, inline bool) *Page {
	page := &Page{Level: level, StartsAtAnchor: true, PageHash: make([]byte, HashSize)}
	for i := range n {
		ent := &Entry{Hash: bytes.Repeat([]byte{byte(i)}, HashSize), Size: 1}
		if i == 0 {
			ent.Anchor = true
		} else {
			ent.Key = fmt.Appendf(nil, "object/%08d/quad/predicate/some-longer-key", i)
		}
		sum := sha256.Sum256(fmt.Append(nil, i))
		ref := &block.BlockRef{Hash: &hash.Hash{HashType: hash.HashType_HashType_SHA256, Hash: sum[:]}}
		switch {
		case level != 0:
			ent.ChildRef = ref
		case i == 0:
		case inline && i%2 == 0:
			ent.ValueIsBlob = true
			ent.ValueBlob = &blob.Blob{TotalSize: 8, RawData: sum[:8]}
		default:
			ent.ValueRef = ref
		}
		page.Entries = append(page.Entries, ent)
		page.Size += ent.Size
	}
	return page
}

// checkPageDecode requires UnmarshalDecodedBlock to match UnmarshalVT on data,
// field by field, and to retain no part of data.
func checkPageDecode(t *testing.T, data []byte) {
	// Decode with both decoders and require the same result.
	t.Helper()
	var want, got Page
	wantErr := want.UnmarshalVT(data)
	gotErr := got.UnmarshalDecodedBlock(data)
	if (wantErr == nil) != (gotErr == nil) {
		t.Fatalf("UnmarshalDecodedBlock error = %v, UnmarshalVT error = %v", gotErr, wantErr)
	}
	if wantErr != nil {
		return
	}
	if !reflect.DeepEqual(&got, &want) {
		t.Fatalf("UnmarshalDecodedBlock = %v, want %v", &got, &want)
	}

	// Overwrite the input: the decoded page must not change.
	for i := range data {
		data[i] ^= 0xff
	}
	if !reflect.DeepEqual(&got, &want) {
		t.Fatal("decoded page aliases its input")
	}
}

// TestPageDecodeMatchesGenerated checks the slab decoder on canonical pages,
// and that canonical pages take the slab path.
func TestPageDecodeMatchesGenerated(t *testing.T) {
	pages := map[string]*Page{
		"empty":    {},
		"leaf":     decodeTestPage(0, 32, false),
		"inline":   decodeTestPage(0, 32, true),
		"internal": decodeTestPage(1, 32, false),
		"bounded": func() *Page {
			// Drop the anchor and bound the page by its keys.
			page := decodeTestPage(1, 3, false)
			page.StartsAtAnchor = false
			page.Entries = page.Entries[1:]
			page.LowerBound = page.Entries[0].Key
			page.UpperBound = []byte("zzz")
			return page
		}(),
	}
	for name, page := range pages {
		t.Run(name, func(t *testing.T) {
			// Encode the page.
			data, err := page.MarshalVT()
			if err != nil {
				t.Fatal(err)
			}

			// Require the slab path, then the same page as UnmarshalVT.
			var slab Page
			if !decodePageSlab(&slab, data) {
				t.Fatal("canonical page left the slab path")
			}
			checkPageDecode(t, data)
		})
	}
}

// TestCursorDecodesPageIntoSlab checks that a page read through a block
// cursor takes the slab decoder: its entries sit in one array.
func TestCursorDecodesPageIntoSlab(t *testing.T) {
	// Store a page.
	ctx := t.Context()
	store := newOkraTestStore()
	ref, _, err := block.PutBlock(ctx, store, decodeTestPage(1, 4, false))
	if err != nil {
		t.Fatal(err)
	}

	// Read it back through a cursor.
	_, cursor := block.NewTransaction(store, nil, ref, nil)
	page, err := block.UnmarshalBlock[Page](ctx, cursor)
	if err != nil {
		t.Fatal(err)
	}

	// Require the entries to sit in one array.
	first := reflect.ValueOf(page.Entries[0]).Pointer()
	size := reflect.TypeFor[Entry]().Size()
	if reflect.ValueOf(page.Entries[3]).Pointer() != first+3*size {
		t.Fatal("cursor read did not decode the page into a slab")
	}
}

// TestPageDecodeAppendIsolated checks that appending to a decoded field does
// not write into the field after it.
func TestPageDecodeAppendIsolated(t *testing.T) {
	// Decode a page.
	data, err := decodeTestPage(0, 4, false).MarshalVT()
	if err != nil {
		t.Fatal(err)
	}
	var page Page
	if err := page.UnmarshalDecodedBlock(data); err != nil {
		t.Fatal(err)
	}

	// Append to a key and require the hash after it unchanged.
	hashBefore := bytes.Clone(page.Entries[1].Hash)
	_ = append(page.Entries[1].Key, bytes.Repeat([]byte{0xee}, 64)...)
	if !bytes.Equal(page.Entries[1].Hash, hashBefore) {
		t.Fatal("append to a key overwrote the entry hash")
	}
}

// TestPageDecodeNonEmptyMerges checks that decoding into a non-empty page
// keeps the generated decoder's merge.
func TestPageDecodeNonEmptyMerges(t *testing.T) {
	// Encode a page.
	data, err := decodeTestPage(0, 4, false).MarshalVT()
	if err != nil {
		t.Fatal(err)
	}

	// Decode it into a set page with both decoders.
	want, got := &Page{Level: 9}, &Page{Level: 9}
	if err := want.UnmarshalVT(data); err != nil {
		t.Fatal(err)
	}
	if err := got.UnmarshalDecodedBlock(data); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("non-empty page decode differs from UnmarshalVT")
	}
}

// FuzzPageDecode checks that the slab decoder matches the generated decoder
// on any input.
func FuzzPageDecode(f *testing.F) {
	// Seed with canonical pages.
	for _, page := range []*Page{
		{},
		decodeTestPage(0, 3, false),
		decodeTestPage(0, 3, true),
		decodeTestPage(1, 3, false),
	} {
		data, err := page.MarshalVT()
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}

	// Repeated, unknown and truncated fields.
	f.Add([]byte{0x08, 0x01, 0x08, 0x02})
	f.Add([]byte{0x3a, 0x02, 0x12, 0x00, 0x3a, 0x02, 0x12, 0x00})
	f.Add([]byte{0x48, 0x01})
	f.Add([]byte{0x3a, 0x04, 0x2a, 0x02, 0x0a, 0x00})
	f.Add([]byte{0x3a, 0x05, 0x2a, 0x03, 0x0a, 0x01, 0x08})
	f.Add([]byte{0x12, 0x05, 0x01})

	// Compare the decoders on each input.
	f.Fuzz(func(t *testing.T, data []byte) {
		checkPageDecode(t, data)
	})
}

// BenchmarkPageDecode compares the slab decoder with the generated decoder.
func BenchmarkPageDecode(b *testing.B) {
	for _, level := range []uint32{0, 1} {
		for _, n := range []int{8, 32, 128} {
			data, err := decodeTestPage(level, n, false).MarshalVT()
			if err != nil {
				b.Fatal(err)
			}
			b.Run(fmt.Sprintf("level=%d/n=%d/generated", level, n), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					var page Page
					if err := page.UnmarshalVT(data); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(fmt.Sprintf("level=%d/n=%d/slab", level, n), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(data)))
				for b.Loop() {
					var page Page
					if err := page.UnmarshalDecodedBlock(data); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
