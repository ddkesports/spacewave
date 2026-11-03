package block_store_writeback

import (
	"context"
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/s4wave/spacewave/db/block"
	"github.com/s4wave/spacewave/net/hash"
)

// TestEvictAdmitPacesBatches checks that an eviction pass asks its admit
// before each batch with the bytes the last one dropped, and that the admit's
// error ends the pass after the batches it let through. The next passes walk
// the rest over several batches, each resuming where the last stopped: every
// entry is evicted or kept exactly once, and a kept entry waits for the pass
// after.
func TestEvictAdmitPacesBatches(t *testing.T) {
	// Open a placed store.
	ctx := t.Context()
	s := newTestEnv(t).newStore(t, true)

	// Write more blocks than three batches evict, and keep every third.
	const total = 3*evictBatchBlocks + 10
	var size int64
	kept := make(map[string]bool)
	for i := range total {
		data := fmt.Appendf(nil, "block-%05d", i)
		size = int64(len(data))
		ref, _, err := s.PutBlock(ctx, data, leafOpts)
		if err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			kept[ref.GetHash().MarshalString()] = true
		}
	}

	// Upload them all.
	remote := newInmemBlockStore()
	uploadCtx, uploadCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- s.Upload(uploadCtx, remote) }()
	for {
		status, changed := s.GetStatus()
		if status.Pending == 0 {
			break
		}
		<-changed
	}
	uploadCancel()
	<-done

	// Admit the first batch and refuse the second.
	var asked []int64
	stop := errors.New("paused")
	admit := func(_ context.Context, lastBytes int64) error {
		if asked = append(asked, lastBytes); len(asked) > 1 {
			return stop
		}
		return nil
	}
	result, err := s.Evict(ctx, remote, 1<<30, nil, admit)
	if !errors.Is(err, stop) {
		t.Fatalf("evict err = %v, want the admit's", err)
	}
	batchBytes := evictBatchBlocks * size
	if result.Evicted != evictBatchBlocks || result.EvictedBytes != batchBytes {
		t.Fatalf("evict = %+v, want one batch", result)
	}
	if len(asked) != 2 || asked[0] != 0 || asked[1] != batchBytes {
		t.Fatalf("admit asked with %v, want [0 %d]", asked, batchBytes)
	}

	// Walk the rest in one pass, keeping every third block. A kept block
	// must not come up again in this pass.
	seen := make(map[string]int)
	keep := func(h *hash.Hash) bool {
		key := h.MarshalString()
		seen[key]++
		return kept[key]
	}
	result, err = s.Evict(ctx, remote, 1<<30, keep, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rest := total - evictBatchBlocks; result.Evicted+result.Requeued != rest || len(seen) != rest {
		t.Fatalf("second pass = %+v over %d entries, want %d", result, len(seen), rest)
	}
	for key, n := range seen {
		if n != 1 {
			t.Fatalf("second pass walked %s %d times", key, n)
		}
	}

	// The next pass drops exactly the blocks the second kept.
	requeued := result.Requeued
	result, err = s.Evict(ctx, remote, 1<<30, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Evicted != requeued || result.Requeued != 0 {
		t.Fatalf("third pass = %+v, want %d evicted", result, requeued)
	}
}

// askedStore counts the blocks a remote is asked about in existence batches.
type askedStore struct {
	block.StoreOps
	asked map[string]int
}

// GetBlockExistsBatch records each ref before asking the wrapped store.
func (a *askedStore) GetBlockExistsBatch(ctx context.Context, refs []*block.BlockRef) ([]bool, error) {
	for _, ref := range refs {
		a.asked[ref.GetHash().MarshalString()]++
	}
	return a.StoreOps.GetBlockExistsBatch(ctx, refs)
}

// TestEvictKeepsUnsafeBlocks tries to make a pass drop a local value the
// remote may not hold or that was just read: a block whose upload is pending
// again, one the remote lost, and one keep reports. Each must still read
// locally after the pass, the kept block must be requeued without asking the
// remote, and only the uploaded unread block may be dropped.
func TestEvictKeepsUnsafeBlocks(t *testing.T) {
	defer func(off bool) { keepFirstOff = off }(keepFirstOff)
	for _, off := range []bool{false, true} {
		keepFirstOff = off
		t.Run(map[bool]string{false: "keep-first", true: "earlier-order"}[off], testEvictKeepsUnsafeBlocks)
	}
}

// testEvictKeepsUnsafeBlocks runs TestEvictKeepsUnsafeBlocks in the order
// keepFirstOff selects; only keep-first skips asking about the kept block.
func testEvictKeepsUnsafeBlocks(t *testing.T) {
	// Write four blocks and upload them.
	ctx := t.Context()
	s := newTestEnv(t).newStore(t, true)
	names := []string{"droppable", "kept", "lost", "pending"}
	refs := make(map[string]*block.BlockRef, len(names))
	for _, name := range names {
		ref, _, err := s.PutBlock(ctx, []byte(name), leafOpts)
		if err != nil {
			t.Fatal(err)
		}
		refs[name] = ref
	}
	inner := newInmemBlockStore()
	drain(t, s, inner)

	// The remote loses one block, and another is written again, so a marker
	// awaits its upload while its eviction entry stays queued.
	if err := inner.RmBlock(ctx, refs["lost"]); err != nil {
		t.Fatal(err)
	}
	if err := s.local.RmBlock(ctx, refs["pending"]); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.PutBlock(ctx, []byte("pending"), leafOpts); err != nil {
		t.Fatal(err)
	}

	// Run a pass that keeps one block.
	remote := &askedStore{StoreOps: inner, asked: make(map[string]int)}
	kept := refs["kept"].GetHash().MarshalString()
	keep := func(h *hash.Hash) bool { return h.MarshalString() == kept }
	result, err := s.Evict(ctx, remote, 1<<30, keep, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Evicted != 1 || result.Requeued != 1 || result.Refused != 2 {
		t.Fatalf("pass = %+v, want 1 evicted, 1 requeued, 2 refused", result)
	}
	if asked := remote.asked[kept] != 0; asked != keepFirstOff {
		t.Fatalf("pass asked the remote about the kept block: %v", asked)
	}

	// Every block but the droppable one still reads locally with its bytes.
	for _, name := range names {
		data, found, err := s.values.GetBlock(ctx, refs[name])
		if err != nil {
			t.Fatal(err)
		}
		if want := name != "droppable"; found != want || (found && string(data) != name) {
			t.Fatalf("%s: local found %v, want %v", name, found, want)
		}
	}
	if found, err := inner.GetBlockExists(ctx, refs["droppable"]); err != nil || !found {
		t.Fatalf("remote lacks the dropped block: %v", err)
	}
}
