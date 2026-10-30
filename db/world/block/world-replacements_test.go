package world_block

import (
	"testing"

	"github.com/s4wave/spacewave/db/block"
	block_mock "github.com/s4wave/spacewave/db/block/mock"
	"github.com/s4wave/spacewave/db/bucket"
	bucket_lookup "github.com/s4wave/spacewave/db/bucket/lookup"
	"github.com/s4wave/spacewave/db/testbed"
	"github.com/s4wave/spacewave/db/world"
	"github.com/sirupsen/logrus"
)

// TestWorldReplacementsMatchDiff commits World operations that write object
// trees through the World, and checks each commit's record against the diff of
// the old and new World roots.
func TestWorldReplacementsMatchDiff(t *testing.T) {
	// Build an empty testbed cursor.
	ctx := t.Context()
	le := logrus.NewEntry(logrus.New())
	tb, err := testbed.NewTestbed(ctx, le)
	if err != nil {
		t.Fatal(err)
	}
	defer tb.Release()
	ocs, err := tb.BuildEmptyCursor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer ocs.Release()

	// Build a World on it whose transactions record into one log.
	log := block.NewReplacementLog(nil)
	ocs.SetReplacementLog(log)
	ws, err := BuildMockWorldState(ctx, le, true, ocs, false)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Discard()
	store := ocs.GetBucket()

	// commit commits the pending operations and checks the interval's record.
	from := ws.GetRootRef()
	commit := func(name string) {
		if err := ws.Commit(ctx); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		to := ws.GetRootRef()
		if err := block_mock.CheckReplacements(ctx, store, from, to, log.Take(to)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		from = to
	}

	// writeChain writes root -> mid -> leaf through the World and returns the
	// object ref of the root.
	writeChain := func(msg string) *bucket.ObjectRef {
		var oref *bucket.ObjectRef
		err := ws.AccessWorldState(ctx, nil, func(bls *bucket_lookup.Cursor) error {
			// Build the three pages under one new root.
			oref = bls.GetRef()
			btx, root := bls.BuildTransactionAtRef(nil, nil)
			root.SetBlock(&block_mock.SubBlock{}, true)
			mid := root.FollowRef(1, nil)
			mid.SetBlock(&block_mock.SubBlock{}, true)
			mid.FollowRef(1, nil).SetBlock(block_mock.NewExample(msg), true)

			// Store them and return the root.
			var err error
			oref.RootRef, _, err = btx.Write(ctx, true)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return oref
	}

	// putRoot puts one block through a World cursor as an object root, the way
	// the Notes object block writes, and returns its object ref.
	putRoot := func(msg string) *bucket.ObjectRef {
		var oref *bucket.ObjectRef
		err := ws.AccessWorldState(ctx, nil, func(bls *bucket_lookup.Cursor) error {
			// Encode the block and put it as the object's root.
			data, err := block_mock.NewExample(msg).MarshalVT()
			if err != nil {
				return err
			}
			oref = bls.GetRef().Clone()
			oref.RootRef, _, err = bls.PutBlock(ctx, data, &block.PutOpts{RefsKnown: true})
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return oref
	}

	// createObject creates an object at a new chain.
	createObject := func(key string) {
		obj, err := ws.CreateObject(ctx, key, writeChain(key))
		world.ReleaseObjectState(obj)
		if err != nil {
			t.Fatal(err)
		}
	}

	// The first commit writes the empty World's pages. Turn the changelog off:
	// its entries name Object blocks the object tree also names, a second
	// parent the record does not count.
	if _, err := ws.SetChangelogDisabled(ctx, true); err != nil {
		t.Fatal(err)
	}
	commit("init")

	// Create two objects with their trees.
	createObject("a")
	createObject("b")
	commit("create")

	// Look up a's tree.
	obj, found, err := ws.GetObject(ctx, "a")
	if err != nil || !found {
		t.Fatalf("get a: found %v: %v", found, err)
	}
	oref, _, err := obj.GetRootRef(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Edit a's leaf through an object transaction the World built.
	err = obj.AccessWorldState(ctx, oref, func(bls *bucket_lookup.Cursor) error {
		// Load the root and middle pages.
		btx, root := bls.BuildTransaction(nil)
		rootBlk, err := root.Unmarshal(ctx, block_mock.NewSubBlockBlock)
		if err != nil {
			return err
		}
		mid := root.FollowRef(1, rootBlk.(*block_mock.SubBlock).GetExamplePtr())
		if _, err := mid.Unmarshal(ctx, block_mock.NewSubBlockBlock); err != nil {
			return err
		}

		// Replace the leaf and store the new tree.
		mid.FollowRef(1, nil).SetBlock(block_mock.NewExample("a edited"), true)
		oref = oref.Clone()
		oref.RootRef, _, err = btx.Write(ctx, true)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	// Point a at the new tree.
	if _, err := obj.SetRootRef(ctx, oref); err != nil {
		t.Fatal(err)
	}
	world.ReleaseObjectState(obj)
	commit("edit")

	// Link a to b in the graph.
	if err := ws.SetGraphQuad(ctx, world.NewGraphQuadWithKeys("a", "<links>", "b", "")); err != nil {
		t.Fatal(err)
	}
	commit("quad")

	// Rename a to c, which moves its quad, then delete b and the quad.
	obj, err = ws.RenameObject(ctx, "a", "c", false)
	world.ReleaseObjectState(obj)
	if err != nil {
		t.Fatal(err)
	}
	commit("rename")
	if err := ws.DeleteGraphQuad(ctx, world.NewGraphQuadWithKeys("c", "<links>", "b", "")); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.DeleteObject(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	commit("delete")

	// Create an object holding one put block, then replace the block.
	obj, err = ws.CreateObject(ctx, "n", putRoot("note"))
	world.ReleaseObjectState(obj)
	if err != nil {
		t.Fatal(err)
	}
	commit("put")

	// Point n at a second put block.
	obj, _, err = ws.GetObject(ctx, "n")
	if err != nil {
		t.Fatal(err)
	}
	_, err = obj.SetRootRef(ctx, putRoot("note edited"))
	world.ReleaseObjectState(obj)
	if err != nil {
		t.Fatal(err)
	}
	commit("put edit")
}
