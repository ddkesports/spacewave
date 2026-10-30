package block_test

import (
	"testing"

	"github.com/s4wave/spacewave/db/block"
	block_mock "github.com/s4wave/spacewave/db/block/mock"
)

// recording is a transaction recording into its own log, with the root and
// middle pages of a chain loaded.
type recording struct {
	tx        *block.Transaction
	log       *block.ReplacementLog
	root, mid *block.Cursor
}

// TestReplacementsMatchDiff walks a chain root -> mid -> leaf through edits,
// moves, removals and root changes, and checks each interval's record against
// the diff of the old and new trees.
func TestReplacementsMatchDiff(t *testing.T) {
	// Every scenario writes to one in-memory store.
	ctx := t.Context()
	store := newGraphTestStore()

	// chain stores root -> mid -> leaf and returns the root.
	chain := func(msg string) *block.BlockRef {
		// Build the three pages under one new root.
		tx, root := block.NewTransaction(store, nil, nil, nil)
		root.SetBlock(&block_mock.SubBlock{}, true)
		mid := root.FollowRef(1, nil)
		mid.SetBlock(&block_mock.SubBlock{}, true)
		mid.FollowRef(1, nil).SetBlock(block_mock.NewExample(msg), true)

		// Store them and return the root.
		ref, _, err := tx.Write(ctx, true)
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}

	// open starts a recording transaction at from and loads root and mid.
	open := func(from *block.BlockRef) *recording {
		// Load the root through a recording transaction.
		r := &recording{log: block.NewReplacementLog(from)}
		r.tx, r.root = block.NewTransaction(store, nil, from, nil)
		r.tx.RecordReplacements(r.log)
		rootBlk, err := r.root.Unmarshal(ctx, block_mock.NewSubBlockBlock)
		if err != nil {
			t.Fatal(err)
		}

		// Load the middle page under it.
		r.mid = r.root.FollowRef(1, rootBlk.(*block_mock.SubBlock).GetExamplePtr())
		if _, err := r.mid.Unmarshal(ctx, block_mock.NewSubBlockBlock); err != nil {
			t.Fatal(err)
		}
		return r
	}

	// check writes the transaction and compares its record with the diff.
	check := func(name string, r *recording, from *block.BlockRef) {
		// The interval's record names exactly what the diff finds dead.
		to, _, err := r.tx.Write(ctx, true)
		if err != nil {
			t.Fatal(err)
		}
		if err := block_mock.CheckReplacements(ctx, store, from, to, r.log.Take(to)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}

		// A write with nothing dirty records nothing.
		to, _, err = r.tx.Write(ctx, false)
		if err != nil {
			t.Fatal(err)
		}
		rep := r.log.Take(to)
		if len(rep.Replaced)+len(rep.Removed) != 0 {
			t.Fatalf("%s: clean interval recorded %d replaced, %d removed", name, len(rep.Replaced), len(rep.Removed))
		}
	}

	// Edit the leaf in place.
	from := chain("edit")
	r := open(from)
	leaf := r.mid.FollowRef(1, nil)
	if _, err := block_mock.UnmarshalExample(ctx, leaf); err != nil {
		t.Fatal(err)
	}
	leaf.SetBlock(block_mock.NewExample("edited"), true)
	check("edit", r, from)

	// Remove the mid subtree without loading the leaf.
	from = chain("remove")
	r = open(from)
	r.root.ClearRef(1)
	rootBlk, _ := r.root.GetBlock()
	rootBlk.(*block_mock.SubBlock).ExamplePtr = nil
	r.root.MarkDirty()
	check("remove", r, from)

	// Replace the mid subtree with a new one by ref.
	from = chain("swap")
	other := chain("other")
	r = open(from)
	r.mid.SetRefAtCursor(other, true)
	check("swap", r, from)

	// Move the leaf under a new middle block.
	from = chain("move")
	r = open(from)
	leaf = r.mid.FollowRef(1, nil)
	if _, err := block_mock.UnmarshalExample(ctx, leaf); err != nil {
		t.Fatal(err)
	}

	// Attach it under a fresh middle page.
	nmid := r.root.Detach(false)
	nmid.ClearAllRefs()
	nmid.SetBlock(&block_mock.SubBlock{}, true)
	nmid.SetRef(1, leaf)
	r.root.SetRef(1, nmid)
	check("move", r, from)

	// Make mid the root.
	from = chain("reroot")
	r = open(from)
	if err := r.tx.SetRoot(r.mid); err != nil {
		t.Fatal(err)
	}
	check("reroot", r, from)

	// Two writes in one interval: the first write's pages die in the second.
	from = chain("twice")
	r = open(from)
	leaf = r.mid.FollowRef(1, nil)
	leaf.SetBlock(block_mock.NewExample("first"), true)
	if _, _, err := r.tx.Write(ctx, false); err != nil {
		t.Fatal(err)
	}
	leaf.SetBlock(block_mock.NewExample("second"), true)
	check("twice", r, from)
}
