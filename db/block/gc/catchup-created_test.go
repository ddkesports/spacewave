package block_gc

import (
	"context"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/s4wave/spacewave/db/kvtx"
	store_kvtx_inmem "github.com/s4wave/spacewave/db/store/kvtx/inmem"
)

// listReadStore counts the child list records its transactions read.
type listReadStore struct {
	kvtx.Store
	// reads counts the Get calls on child list keys.
	reads atomic.Int64
}

// NewTransaction wraps the transaction to count its child list reads.
func (s *listReadStore) NewTransaction(ctx context.Context, write bool) (kvtx.Tx, error) {
	tx, err := s.Store.NewTransaction(ctx, write)
	if err != nil {
		return nil, err
	}
	return &listReadTx{Tx: tx, reads: &s.reads}, nil
}

// listReadTx counts the child list records it reads.
type listReadTx struct {
	kvtx.Tx
	reads *atomic.Int64
}

// Get counts a read of a child list record.
func (t *listReadTx) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	if len(key) != 0 && key[0] == kindChildren {
		t.reads.Add(1)
	}
	return t.Tx.Get(ctx, key)
}

// TestCatchUpSkipsCreatedLists checks the stall of October 27 on solid: a
// catch-up read the child list of every block a write created during the
// pass, and while writes created blocks faster than it read them, its
// records left never shrank and no sweep ran. Each such write logged the
// children it named, so the catch-up now adds the blocks it created without
// reading their lists, and reads only the lists of blocks that existed
// before the pass, such as an unreached block a new block revives.
func TestCatchUpSkipsCreatedLists(t *testing.T) {
	// Open a graph over a store that counts list reads.
	ctx := context.Background()
	store := &listReadStore{Store: store_kvtx_inmem.NewStore()}
	rg := NewRefGraph(store)
	t.Cleanup(func() { rg.Close() })

	// Record an owned root, and an unreached parent whose child nothing
	// else names.
	owner := BucketIRI("owner")
	root, leaf := BlockIRI(testBlockRef(t, "old-root")), BlockIRI(testBlockRef(t, "old-leaf"))
	parent, child := BlockIRI(testBlockRef(t, "old-parent")), BlockIRI(testBlockRef(t, "old-child"))
	apply := func(edges ...RefEdge) {
		t.Helper()
		if err := rg.ApplyRefBatch(ctx, edges, nil); err != nil {
			t.Fatal(err)
		}
	}
	apply(RefEdge{Subject: owner, Object: root}, RefEdge{Subject: root, Object: leaf}, RefEdge{Subject: NodeUnreferenced, Object: leaf})
	apply(RefEdge{Subject: NodeUnreferenced, Object: parent}, RefEdge{Subject: parent, Object: child}, RefEdge{Subject: NodeUnreferenced, Object: child})

	// Open a pass and mark: the parent and its child are unreached.
	if err := rg.BeginBlockPass(ctx); err != nil {
		t.Fatal(err)
	}
	mark, err := rg.MarkBlocksScan(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(mark.Unreached, parent) || !slices.Contains(mark.Unreached, child) {
		t.Fatalf("unreached %v, want the parent and its child", mark.Unreached)
	}

	// Writes create owned trees, each a new block naming new leaves.
	var created []string
	for i := range 50 {
		tree := BlockIRI(testBlockRef(t, "new-tree-"+strconv.Itoa(i)))
		edges := []RefEdge{{Subject: owner, Object: tree}, {Subject: NodeUnreferenced, Object: tree}}
		for j := range 20 {
			leaf := BlockIRI(testBlockRef(t, "new-leaf-"+strconv.Itoa(i)+"-"+strconv.Itoa(j)))
			edges = append(edges, RefEdge{Subject: tree, Object: leaf}, RefEdge{Subject: NodeUnreferenced, Object: leaf})
			created = append(created, leaf)
		}
		apply(edges...)
		created = append(created, tree)
	}

	// One more names the unreached parent from a new owned block.
	reviver := BlockIRI(testBlockRef(t, "reviver"))
	apply(RefEdge{Subject: owner, Object: reviver}, RefEdge{Subject: NodeUnreferenced, Object: reviver}, RefEdge{Subject: reviver, Object: parent})
	created = append(created, reviver)

	// The catch-up reaches every block, reading only the lists of the
	// revived parent and its child.
	store.reads.Store(0)
	if _, err := rg.CatchUpAdopted(ctx, mark); err != nil {
		t.Fatal(err)
	}
	if n := store.reads.Load(); n != 2 {
		t.Fatalf("catch-up read %d child lists, want 2", n)
	}
	for _, b := range append(created, parent, child) {
		if !mark.Live.Has(b) {
			t.Fatalf("block %q not live after the catch-up", b)
		}
	}

	// The rescue finishes with nothing unknown, so the sweep removes no
	// block the writes reached.
	rescued, done, err := rg.RescueAdopted(ctx, mark, nil)
	if err != nil || !done || rescued != 0 || mark.Unknown != 0 {
		t.Fatalf("rescue rescued=%d done=%v unknown=%d err=%v", rescued, done, mark.Unknown, err)
	}
	if err := rg.EndBlockPass(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestPromoteCreatedUnread checks that a block pass promotes the blocks
// writes created during it, whose lists it never read: they are live and
// not settled, so they leave the young log, and a minor pass after it marks
// no young block and leaves nothing live unreached.
func TestPromoteCreatedUnread(t *testing.T) {
	// A graph born empty is minor-ready.
	ctx := context.Background()
	store := &listReadStore{Store: store_kvtx_inmem.NewStore()}
	rg := NewRefGraph(store)
	t.Cleanup(func() { rg.Close() })
	if ready, err := rg.ClaimYoungReady(ctx); err != nil || !ready {
		t.Fatalf("claim on an empty graph: ready=%v err=%v", ready, err)
	}

	// Record an owned root.
	owner := BucketIRI("owner")
	apply := func(edges ...RefEdge) {
		t.Helper()
		if err := rg.ApplyRefBatch(ctx, edges, nil); err != nil {
			t.Fatal(err)
		}
	}
	root := BlockIRI(testBlockRef(t, "promote-root"))
	apply(RefEdge{Subject: owner, Object: root}, RefEdge{Subject: NodeUnreferenced, Object: root})

	// A pass marks, then a write creates an owned tree.
	bound, err := rg.YoungPosition(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := rg.BeginBlockPass(ctx); err != nil {
		t.Fatal(err)
	}
	mark, err := rg.MarkBlocksScan(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	tree, leaf := BlockIRI(testBlockRef(t, "promote-tree")), BlockIRI(testBlockRef(t, "promote-leaf"))
	apply(RefEdge{Subject: owner, Object: tree}, RefEdge{Subject: NodeUnreferenced, Object: tree}, RefEdge{Subject: tree, Object: leaf}, RefEdge{Subject: NodeUnreferenced, Object: leaf})

	// The rescue reaches the tree without reading a list, and the pass
	// promotes in full.
	store.reads.Store(0)
	if _, done, err := rg.RescueAdopted(ctx, mark, nil); err != nil || !done {
		t.Fatalf("rescue done=%v err=%v", done, err)
	}
	if n := store.reads.Load(); n != 0 || !mark.Live.Has(tree) || !mark.Live.Has(leaf) {
		t.Fatalf("rescue read %d lists, tree live %v, leaf live %v", n, mark.Live.Has(tree), mark.Live.Has(leaf))
	}
	for done := false; !done; {
		if done, err = rg.PromoteYoung(ctx, mark, bound, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := rg.EndBlockPass(ctx); err != nil {
		t.Fatal(err)
	}

	// A minor pass finds the tree old: no young block, nothing unreached.
	if err := rg.BeginBlockPass(ctx); err != nil {
		t.Fatal(err)
	}
	young, err := rg.MarkYoung(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if young.YoungBlocks != 0 || len(young.Unreached) != 0 {
		t.Fatalf("minor mark young=%d unreached=%v, want none", young.YoungBlocks, young.Unreached)
	}

	// A tree a write creates during the minor pass is reached without a
	// list read.
	newTree, newLeaf := BlockIRI(testBlockRef(t, "minor-tree")), BlockIRI(testBlockRef(t, "minor-leaf"))
	apply(RefEdge{Subject: owner, Object: newTree}, RefEdge{Subject: NodeUnreferenced, Object: newTree}, RefEdge{Subject: newTree, Object: newLeaf}, RefEdge{Subject: NodeUnreferenced, Object: newLeaf})
	store.reads.Store(0)
	if _, done, err := rg.RescueAdopted(ctx, young, nil); err != nil || !done {
		t.Fatalf("minor rescue done=%v err=%v", done, err)
	}
	if n := store.reads.Load(); n != 0 || !young.Live.Has(newTree) || !young.Live.Has(newLeaf) {
		t.Fatalf("minor rescue read %d lists, tree live %v, leaf live %v", n, young.Live.Has(newTree), young.Live.Has(newLeaf))
	}
	if err := rg.EndBlockPass(ctx); err != nil {
		t.Fatal(err)
	}
}
