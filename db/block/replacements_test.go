package block_test

import (
	"context"
	"testing"

	"github.com/s4wave/spacewave/db/block"
	block_mock "github.com/s4wave/spacewave/db/block/mock"
)

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

	// open starts a recording transaction and loads root and mid.
	open := func(from *block.BlockRef) (*block.Transaction, *block.Cursor, *block.Cursor) {
		// Load the root through a recording transaction.
		tx, root := block.NewTransaction(store, nil, from, nil)
		tx.RecordReplacements()
		rootBlk, err := root.Unmarshal(ctx, block_mock.NewSubBlockBlock)
		if err != nil {
			t.Fatal(err)
		}

		// Load the middle page under it.
		mid := root.FollowRef(1, rootBlk.(*block_mock.SubBlock).GetExamplePtr())
		if _, err := mid.Unmarshal(ctx, block_mock.NewSubBlockBlock); err != nil {
			t.Fatal(err)
		}
		return tx, root, mid
	}

	// check writes tx and compares its record with the diff.
	check := func(name string, tx *block.Transaction, from *block.BlockRef) *block.BlockRef {
		to, _, err := tx.Write(ctx, true)
		if err != nil {
			t.Fatal(err)
		}
		if err := block_mock.CheckReplacements(ctx, store, from, to, tx.TakeReplacements()); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return to
	}

	// Edit the leaf in place.
	from := chain("edit")
	tx, _, mid := open(from)
	leaf := mid.FollowRef(1, nil)
	if _, err := block_mock.UnmarshalExample(ctx, leaf); err != nil {
		t.Fatal(err)
	}
	leaf.SetBlock(block_mock.NewExample("edited"), true)
	check("edit", tx, from)

	// Remove the mid subtree without loading the leaf.
	from = chain("remove")
	tx, root, _ := open(from)
	root.ClearRef(1)
	rootBlk, _ := root.GetBlock()
	rootBlk.(*block_mock.SubBlock).ExamplePtr = nil
	root.MarkDirty()
	check("remove", tx, from)

	// Replace the mid subtree with a new one by ref.
	from = chain("swap")
	other := chain("other")
	tx, _, mid = open(from)
	mid.SetRefAtCursor(other, true)
	check("swap", tx, from)

	// Move the leaf under a new middle block.
	from = chain("move")
	tx, root, mid = open(from)
	leaf = mid.FollowRef(1, nil)
	if _, err := block_mock.UnmarshalExample(ctx, leaf); err != nil {
		t.Fatal(err)
	}

	// Attach it under a fresh middle page.
	nmid := root.Detach(false)
	nmid.ClearAllRefs()
	nmid.SetBlock(&block_mock.SubBlock{}, true)
	nmid.SetRef(1, leaf)
	root.SetRef(1, nmid)
	check("move", tx, from)

	// Make mid the root.
	from = chain("reroot")
	tx, _, mid = open(from)
	if err := tx.SetRoot(mid); err != nil {
		t.Fatal(err)
	}
	check("reroot", tx, from)

	// Two writes in one interval: the first write's pages die in the second.
	from = chain("twice")
	tx, _, mid = open(from)
	leaf = mid.FollowRef(1, nil)
	leaf.SetBlock(block_mock.NewExample("first"), true)
	if _, _, err := tx.Write(ctx, false); err != nil {
		t.Fatal(err)
	}
	leaf.SetBlock(block_mock.NewExample("second"), true)
	check("twice", tx, from)
	assertNoReplacements(t, ctx, tx)
}

// assertNoReplacements checks that a clean interval records nothing.
func assertNoReplacements(t *testing.T, ctx context.Context, tx *block.Transaction) {
	t.Helper()
	if _, _, err := tx.Write(ctx, false); err != nil {
		t.Fatal(err)
	}
	rep := tx.TakeReplacements()
	if len(rep.Replaced)+len(rep.Removed) != 0 {
		t.Fatalf("clean interval recorded %d replaced, %d removed", len(rep.Replaced), len(rep.Removed))
	}
}
