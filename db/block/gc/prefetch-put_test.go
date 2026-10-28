package block_gc

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/s4wave/spacewave/db/block"
	block_store_kvtx "github.com/s4wave/spacewave/db/block/store/kvtx"
	"github.com/s4wave/spacewave/db/kvtx"
	store_kvkey "github.com/s4wave/spacewave/db/store/kvkey"
	store_kvtx_inmem "github.com/s4wave/spacewave/db/store/kvtx/inmem"
)

// readRecorder wraps a store and records the keys its transactions read: the
// keys read transactions prefetch, and the keys write transactions read.
type readRecorder struct {
	kvtx.Store
	// mu guards the key sets.
	mu sync.Mutex
	// prefetched holds the keys read transactions prefetched.
	prefetched map[string]struct{}
	// read holds the keys and prefixes write transactions read.
	read map[string]struct{}
}

// NewTransaction wraps the store's transaction to record its reads.
func (r *readRecorder) NewTransaction(ctx context.Context, write bool) (kvtx.Tx, error) {
	tx, err := r.Store.NewTransaction(ctx, write)
	if err != nil {
		return nil, err
	}
	return &recordingTx{Tx: tx, r: r, write: write}, nil
}

// note records key in the set the transaction's kind fills.
func (r *readRecorder) note(write, prefetch bool, key []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case !write && prefetch:
		r.prefetched[string(key)] = struct{}{}
	case write && !prefetch:
		r.read[string(key)] = struct{}{}
	}
}

// recordingTx records the keys a transaction reads.
type recordingTx struct {
	kvtx.Tx
	r     *readRecorder
	write bool
}

// Prefetch records the keys a read transaction loads.
func (t *recordingTx) Prefetch(ctx context.Context, keys [][]byte) error {
	for _, key := range keys {
		t.r.note(t.write, true, key)
	}
	return kvtx.Prefetch(ctx, t.Tx, keys)
}

// Get records the key a write transaction reads.
func (t *recordingTx) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	t.r.note(t.write, false, key)
	return t.Tx.Get(ctx, key)
}

// Exists records the key a write transaction checks.
func (t *recordingTx) Exists(ctx context.Context, key []byte) (bool, error) {
	t.r.note(t.write, false, key)
	return t.Tx.Exists(ctx, key)
}

// Iterate records the prefix a write transaction walks.
func (t *recordingTx) Iterate(ctx context.Context, prefix []byte, sort, reverse bool) kvtx.Iterator {
	t.r.note(t.write, false, prefix)
	return t.Tx.Iterate(ctx, prefix, sort, reverse)
}

// TestPrefetchPutBatchCoversFlush checks that PrefetchPutBatch loads every
// ref graph record that the parent's put and flush of the same entries then
// read under the writer: new blocks, an existing block written again with
// its refs, an existing block it moves the parent's hold off, and a
// tombstone.
func TestPrefetchPutBatchCoversFlush(t *testing.T) {
	// Open a block store and a recorded ref graph in separate stores.
	ctx := context.Background()
	blocks := block_store_kvtx.NewKVTxBlock(store_kvkey.NewDefaultKVKey(), store_kvtx_inmem.NewStore(), 0, false)
	rec := &readRecorder{Store: store_kvtx_inmem.NewStore(), prefetched: map[string]struct{}{}, read: map[string]struct{}{}}
	rg := NewRefGraph(rec)
	t.Cleanup(func() { rg.Close() })
	const owner = "bucket/owner"

	// Build blocks to write: old ones the first put holds, new ones the
	// second adds.
	ref := func(i int) (*block.BlockRef, []byte) {
		data := []byte("prefetch put block " + strconv.Itoa(i))
		r, err := block.BuildBlockRef(data, nil)
		if err != nil {
			t.Fatal(err)
		}
		return r, data
	}
	old0, oldData0 := ref(0)
	old1, oldData1 := ref(1)
	old2, oldData2 := ref(2)
	new0, newData0 := ref(3)
	new1, newData1 := ref(4)

	// Hold the old blocks under the owner.
	put := func(entries []*block.PutBatchEntry) {
		if err := rg.AddRef(ctx, NodeGCRoot, owner); err != nil {
			t.Fatal(err)
		}
		gc := NewGCStoreOpsWithParent(blocks, rg, owner)
		if _, err := gc.PutBlockBatch(ctx, entries); err != nil {
			t.Fatal(err)
		}
		if err := gc.FlushPending(ctx); err != nil {
			t.Fatal(err)
		}
	}
	put([]*block.PutBatchEntry{
		{Ref: old0, Data: oldData0, RefsKnown: true},
		{Ref: old1, Data: oldData1, Refs: []*block.BlockRef{old0}, RefsKnown: true},
		{Ref: old2, Data: oldData2, RefsKnown: true},
	})

	// Prefetch the second put, then record what its flush reads.
	second := []*block.PutBatchEntry{
		{Ref: new0, Data: newData0, Refs: []*block.BlockRef{old1}, RefsKnown: true},
		{Ref: new1, Data: newData1, Refs: []*block.BlockRef{new0, old2}, RefsKnown: true},
		{Ref: old1, Data: oldData1, Refs: []*block.BlockRef{old0}, RefsKnown: true},
		{Ref: old2, Tombstone: true},
	}
	rec.prefetched, rec.read = make(map[string]struct{}), make(map[string]struct{})
	if err := rg.PrefetchPutBatch(ctx, owner, second); err != nil {
		t.Fatal(err)
	}
	put(second)

	// Every key the flush read under the writer was prefetched.
	if len(rec.read) == 0 {
		t.Fatal("the flush read no ref graph record")
	}
	for key := range rec.read {
		if _, ok := rec.prefetched[key]; !ok {
			t.Errorf("flush read %q, which the prefetch did not load", key)
		}
	}
}
