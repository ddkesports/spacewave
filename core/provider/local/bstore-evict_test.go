package provider_local

import (
	"testing"
	"time"

	block_store_inmem "github.com/s4wave/spacewave/db/block/store/inmem"
	block_store_writeback "github.com/s4wave/spacewave/db/block/store/writeback"
	store_kvkey "github.com/s4wave/spacewave/db/store/kvkey"
	store_kvtx_inmem "github.com/s4wave/spacewave/db/store/kvtx/inmem"
	"github.com/s4wave/spacewave/net/hash"
)

// TestReadSetKeepsRecentReads checks that eviction keeps a read block across
// passes until two generations of readKeepAge have turned.
func TestReadSetKeepsRecentReads(t *testing.T) {
	// Record a read.
	ctx := t.Context()
	now := time.Now()
	r := readSet{clock: func() time.Time { return now }}
	r.enabled.Store(true)
	read, other := hash.NewHash(hash.HashType_HashType_SHA256, []byte("read")), hash.NewHash(hash.HashType_HashType_SHA256, []byte("other"))
	r.add(read)

	// Passes in quick succession keep it.
	take := func() func(*hash.Hash) bool {
		keep, err := r.take(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return keep
	}
	for range 3 {
		if keep := take(); !keep(read) || keep(other) {
			t.Fatal("pass did not keep exactly the read block")
		}
	}

	// One turn keeps it in the previous generation, the second drops it.
	now = now.Add(readKeepAge)
	if !take()(read) {
		t.Fatal("first turn dropped the read block")
	}
	now = now.Add(readKeepAge)
	if take()(read) {
		t.Fatal("second turn kept the read block")
	}

	// A generation idle for two ages may hold a read from just before the
	// pass, so the pass keeps it, and the next age drops it.
	r.add(read)
	take()
	now = now.Add(2 * readKeepAge)
	if !take()(read) {
		t.Fatal("pass after two idle ages dropped a read of the idle generation")
	}
	now = now.Add(readKeepAge)
	if take()(read) {
		t.Fatal("pass an age after the idle turn kept the read block")
	}
}

// TestReadSetKeepsReadsAcrossRestart checks that the saved reads keep their
// blocks after a restart, through one turn, and that the second turn drops
// them from the store too.
func TestReadSetKeepsReadsAcrossRestart(t *testing.T) {
	// Open a writeback store to save the reads to.
	ctx := t.Context()
	local := block_store_inmem.NewInmemBlock(store_kvkey.NewDefaultKVKey(), store_kvtx_inmem.NewStore(), 0, false)
	wb, err := block_store_writeback.NewStore(ctx, local, local, store_kvtx_inmem.NewStore(), "test/markers", true)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	restart := func() *readSet {
		r := &readSet{clock: func() time.Time { return now }}
		r.enabled.Store(true)
		if err := r.load(ctx, wb); err != nil {
			t.Fatal(err)
		}
		return r
	}
	take := func(r *readSet) func(*hash.Hash) bool {
		keep, err := r.take(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return keep
	}

	// A read saved before a restart keeps its block after it.
	read, later, other := hash.NewHash(hash.HashType_HashType_SHA256, []byte("read")), hash.NewHash(hash.HashType_HashType_SHA256, []byte("later")), hash.NewHash(hash.HashType_HashType_SHA256, []byte("other"))
	r := restart()
	r.add(read)
	if err := r.save(ctx); err != nil {
		t.Fatal(err)
	}
	r = restart()
	if keep := take(r); !keep(read) || keep(other) {
		t.Fatal("restart did not keep exactly the saved read")
	}

	// After one turn and a restart, the previous generation still keeps it,
	// and a read since the turn is kept too.
	now = now.Add(readKeepAge)
	take(r)
	r.add(later)
	if err := r.save(ctx); err != nil {
		t.Fatal(err)
	}
	r = restart()
	if keep := take(r); !keep(read) || !keep(later) {
		t.Fatal("restart after a turn dropped a saved read")
	}

	// The second turn drops the first read, and the store forgets it.
	now = now.Add(readKeepAge)
	if keep := take(r); keep(read) || !keep(later) {
		t.Fatal("second turn did not drop only the first read")
	}
	r = restart()
	if keep := take(r); keep(read) || !keep(later) {
		t.Fatal("store kept the dropped generation")
	}
}

// TestReadSetExpiresDuringPass checks that a pass's keep test stops keeping a
// block only the previous generation read once that generation ended
// readKeepAge ago, without waiting for the next pass to turn, and keeps a
// block the current generation read.
func TestReadSetExpiresDuringPass(t *testing.T) {
	// Read a block, turn it into the previous generation, then read another.
	ctx := t.Context()
	now := time.Now()
	r := readSet{clock: func() time.Time { return now }}
	r.enabled.Store(true)
	old, fresh := hash.NewHash(hash.HashType_HashType_SHA256, []byte("old")), hash.NewHash(hash.HashType_HashType_SHA256, []byte("fresh"))
	r.add(old)
	if _, err := r.take(ctx); err != nil {
		t.Fatal(err)
	}
	now = now.Add(readKeepAge)
	keep, err := r.take(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r.add(fresh)
	if !keep(old) || !keep(fresh) {
		t.Fatal("pass right after the turn dropped a read block")
	}

	// The pass runs on past readKeepAge since the turn.
	now = now.Add(readKeepAge)
	if keep(old) {
		t.Fatal("long pass kept a block last read over readKeepAge ago")
	}
	if !keep(fresh) {
		t.Fatal("long pass dropped a block of the current generation")
	}
}

// TestReadSetLegacyOrder checks that the earlier eviction order keeps a block
// the previous generation read until the pass ends, and that a gap of two keep
// ages drops both generations, as before keep-first eviction.
func TestReadSetLegacyOrder(t *testing.T) {
	defer func(legacy bool) { legacyReads = legacy }(legacyReads)
	legacyReads = true

	// A read turned into the previous generation stays kept through a long pass.
	ctx := t.Context()
	now := time.Now()
	r := readSet{clock: func() time.Time { return now }}
	r.enabled.Store(true)
	read := hash.NewHash(hash.HashType_HashType_SHA256, []byte("read"))
	r.add(read)
	if _, err := r.take(ctx); err != nil {
		t.Fatal(err)
	}
	now = now.Add(readKeepAge)
	keep, err := r.take(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(readKeepAge)
	if !keep(read) {
		t.Fatal("earlier order expired a read during the pass")
	}

	// A fresh read is dropped after two idle keep ages.
	r.add(read)
	now = now.Add(2 * readKeepAge)
	keep, err = r.take(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if keep(read) {
		t.Fatal("earlier order kept a read after two idle keep ages")
	}
}
