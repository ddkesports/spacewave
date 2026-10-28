package kvtx

import (
	"testing"

	"github.com/s4wave/spacewave/db/block"
	"github.com/s4wave/spacewave/db/volume"
)

// TestFlowsCountNewBlocksByPath proves each write path counts the blocks it
// stored that the volume did not hold, so the flows name the caller that
// grows the storage.
func TestFlowsCountNewBlocksByPath(t *testing.T) {
	// A direct write counts once; writing the same block again does not.
	v, _ := newPublicationTestVolume(t)
	ctx := t.Context()
	direct := []byte("direct block")
	for range 2 {
		if _, _, err := v.PutBlock(ctx, direct, nil); err != nil {
			t.Fatal(err)
		}
	}

	// An owned write and a cache fill count under their own paths, and a
	// repeated fill does not count.
	owned := []byte("owned block")
	if _, _, err := v.PrepareOwnedBlock(ctx, "bucket", owned, nil); err != nil {
		t.Fatal(err)
	}
	fill := []byte("filled block")
	for range 2 {
		if _, _, err := v.PrepareOwnedBlock(ctx, "bucket", fill, &block.PutOpts{CacheFill: true}); err != nil {
			t.Fatal(err)
		}
	}

	// A stage writes a child and a root.
	_, release, _, _ := stageBuild(t, v, "flows")
	defer release()
	staged := len("flows child") + len("flows root")

	// A bucket's roots carry their entry.
	rootData := []byte("root entry")
	rootRef, err := block.BuildBlockRef(rootData, nil)
	if err != nil {
		t.Fatal(err)
	}
	entries := []*block.PutBatchEntry{{Ref: rootRef, Data: rootData}}
	if err := v.SetBucketRoots(ctx, "bucket", entries, []block.NamedRoot{{Name: "head", Ref: rootRef}}); err != nil {
		t.Fatal(err)
	}

	// A committed publication counts its entry.
	p := publicationFor(t, "store", "", "one")
	if err := awaitPublication(t, submitPublication(t, v, p)); err != nil {
		t.Fatal(err)
	}

	// Each path reports exactly its own blocks.
	want := []volume.PutFlow{
		{Path: "direct", Blocks: 1, Bytes: uint64(len(direct))},
		{Path: "publication", Blocks: 1, Bytes: uint64(len(p.Entries[0].Data))},
		{Path: "owned", Blocks: 1, Bytes: uint64(len(owned))},
		{Path: "fill", Blocks: 1, Bytes: uint64(len(fill))},
		{Path: "staged", Blocks: 2, Bytes: uint64(staged)},
		{Path: "roots", Blocks: 1, Bytes: uint64(len(rootData))},
	}
	flows := v.GetFlows()
	if flows.VolumeID != v.GetID() || len(flows.Puts) != len(want) {
		t.Fatalf("flows = %+v; want volume %s with %d paths", flows, v.GetID(), len(want))
	}
	for i, w := range want {
		if flows.Puts[i] != w {
			t.Fatalf("path %d = %+v; want %+v", i, flows.Puts[i], w)
		}
	}
}
