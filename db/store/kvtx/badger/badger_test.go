package store_kvtx_badger

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"

	bdb "github.com/dgraph-io/badger/v4"
	"github.com/s4wave/spacewave/db/block"
	block_store_kvtx "github.com/s4wave/spacewave/db/block/store/kvtx"
	"github.com/s4wave/spacewave/db/kvtx"
	store_kvkey "github.com/s4wave/spacewave/db/store/kvkey"
	store_kvtx "github.com/s4wave/spacewave/db/store/kvtx"
	kvtx_vlogger "github.com/s4wave/spacewave/db/store/kvtx/vlogger"
	store_test "github.com/s4wave/spacewave/db/store/test"
	"github.com/sirupsen/logrus"
)

// TestBadger tests all tests on top of badger.
func TestBadger(t *testing.T) {
	// Prepare the logger and key encoding for the Badger store contracts.
	ctx := context.Background()
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	le := logrus.NewEntry(log)
	kvkey, err := store_kvkey.NewKVKey(store_kvkey.DefaultConfig())
	if err != nil {
		t.Fatal(err.Error())
	}

	// Open an in-memory Badger store for the storage contract suite.
	o := bdb.DefaultOptions("").WithInMemory(true)
	db, err := Open(o, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	defer db.db.Close()

	// Run the storage contracts through the Badger transaction adapter.
	ktx := store_kvtx.NewKVTx(
		kvkey,
		kvtx_vlogger.NewVLogger(le, db),
		nil,
	).(*store_kvtx.KVTx)
	if err := store_test.TestAll(ctx, ktx); err != nil {
		t.Fatal(err.Error())
	}
}

func TestCommitConflictIsInvalidSnapshot(t *testing.T) {
	// Open an in-memory Badger database for conflicting snapshots.
	db, err := bdb.Open(bdb.DefaultOptions("").WithInMemory(true))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Commit the initial key that both Badger snapshots will observe.
	ctx := context.Background()
	seed := db.NewTransaction(true)
	if err := seed.Set([]byte("key"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := seed.Commit(); err != nil {
		t.Fatal(err)
	}

	// Read the key in the first Badger transaction before a competing write.
	firstStore := NewStore(db)
	firstStore.writeMtx.Lock()
	first := firstStore.newTx(db.NewTransaction(true), true)
	defer first.Discard()
	if _, _, err := first.Get(ctx, []byte("key")); err != nil {
		t.Fatal(err)
	}

	// Commit a competing Badger transaction that changes the same key.
	secondStore := NewStore(db)
	secondStore.writeMtx.Lock()
	second := secondStore.newTx(db.NewTransaction(true), true)
	if err := second.Set(ctx, []byte("key"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := second.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify the stale Badger transaction reports an invalid snapshot on commit.
	if err := first.Set(ctx, []byte("key"), []byte("three")); err != nil {
		t.Fatal(err)
	}
	err = first.Commit(ctx)
	if !errors.Is(err, kvtx.ErrInvalidSnapshot) {
		t.Fatalf("commit error = %v, want ErrInvalidSnapshot", err)
	}
}

// TestPutBlockBatchSplitsFullTransaction writes a block batch larger than one
// Badger transaction holds and checks that every block is stored.
func TestPutBlockBatchSplitsFullTransaction(t *testing.T) {
	// Open a store whose transactions hold about 150 KiB.
	ctx := context.Background()
	db, err := Open(bdb.DefaultOptions("").WithInMemory(true).WithMemTableSize(1<<20).WithValueThreshold(64<<10).WithLogger(nil), false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.db.Close()
	blocks := block_store_kvtx.NewKVTxBlock(store_kvkey.NewDefaultKVKey(), db, 0, false)

	// Write 1 MiB of distinct blocks in one batch.
	entries := make([]*block.PutBatchEntry, 64)
	for i := range entries {
		data := bytes.Repeat([]byte{byte(i)}, 16<<10)
		ref, err := block.BuildBlockRef(data, &block.PutOpts{})
		if err != nil {
			t.Fatal(err)
		}
		entries[i] = &block.PutBatchEntry{Ref: ref, Data: data}
	}
	if _, err := blocks.PutBlockBatch(ctx, entries); err != nil {
		t.Fatal(err)
	}

	// Every block reads back.
	for i, entry := range entries {
		data, found, err := blocks.GetBlock(ctx, entry.Ref)
		if err != nil || !found || !bytes.Equal(data, entry.Data) {
			t.Fatalf("block %d: found=%v err=%v", i, found, err)
		}
	}
}

// TestStoreDurability walks one on-disk store through its commit modes.
func TestStoreDurability(t *testing.T) {
	// Open a store on disk with values in the value log.
	ctx := context.Background()
	s, err := Open(bdb.DefaultOptions(t.TempDir()).WithValueThreshold(64).WithLogger(nil), false)
	if err != nil {
		t.Fatal(err)
	}
	set := func(ctx context.Context, key string, commit func(*Tx, context.Context) error) {
		t.Helper()
		tx, err := s.NewTransaction(ctx, true)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Discard()
		if err := tx.Set(ctx, []byte(key), bytes.Repeat([]byte{1}, 256)); err != nil {
			t.Fatal(err)
		}
		if err := commit(tx.(*Tx), ctx); err != nil {
			t.Fatal(err)
		}
	}

	// A full commit is durable when it returns.
	set(ctx, "full", (*Tx).Commit)
	if d, o := s.durable.Load(), s.ordered.Load(); d != o || o != 1 {
		t.Fatalf("full commit: durable=%d ordered=%d", d, o)
	}

	// Ordered commits, by call or by context, wait for the deadline flush.
	set(ctx, "ordered", (*Tx).CommitOrdered)
	set(kvtx.WithOrderedCommitContext(ctx), "ordered-ctx", (*Tx).Commit)
	if d, o := s.durable.Load(), s.ordered.Load(); d != 1 || o != 3 {
		t.Fatalf("ordered commits: durable=%d ordered=%d", d, o)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*SyncDeadline)
	defer cancel()
	if err := s.WaitDurable(waitCtx); err != nil {
		t.Fatal(err)
	}
	if d := s.durable.Load(); d != 3 {
		t.Fatalf("after deadline: durable=%d", d)
	}

	// A write transaction that changed nothing counts no commit.
	tx, err := s.NewTransaction(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if o := s.ordered.Load(); o != 3 {
		t.Fatalf("empty commit: ordered=%d", o)
	}

	// A finished transaction refuses a commit, and a read transaction a write.
	tx, err = s.NewTransaction(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(ctx, []byte("k"), []byte("v")); !errors.Is(err, kvtx.ErrNotWrite) {
		t.Fatalf("read tx set: %v", err)
	}
	tx.Discard()
	if err := tx.Commit(ctx); !errors.Is(err, kvtx.ErrDiscarded) {
		t.Fatalf("commit after discard: %v", err)
	}

	// Scanned keys outlive the scan, and Size counts every key.
	tx, err = s.NewTransaction(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Discard()
	var keys [][]byte
	if err := tx.ScanPrefixKeys(ctx, []byte("ordered"), func(key []byte) error {
		keys = append(keys, key)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || string(keys[0]) != "ordered" || string(keys[1]) != "ordered-ctx" {
		t.Fatalf("scanned keys: %q", keys)
	}
	if size, err := tx.Size(ctx); err != nil || size != 3 {
		t.Fatalf("size=%d err=%v", size, err)
	}
	tx.Discard()

	// Close flushes a pending ordered commit.
	set(ctx, "pending", (*Tx).CommitOrdered)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if d, o := s.durable.Load(), s.ordered.Load(); d != o {
		t.Fatalf("after close: durable=%d ordered=%d", d, o)
	}
}

// killWriterDirEnv names the store directory of the writer process that
// TestKilledWriterKeepsCommits starts.
const killWriterDirEnv = "SPACEWAVE_BADGER_KILL_WRITER_DIR"

// TestKilledWriterKeepsCommits kills a writer process and checks that every
// commit it reported, across memtable and value log rotation, reads back.
func TestKilledWriterKeepsCommits(t *testing.T) {
	// Act as the writer when started by this test.
	opts := func(dir string) bdb.Options {
		return bdb.DefaultOptions(dir).
			WithMemTableSize(1 << 20).
			WithValueLogFileSize(1 << 20).
			WithValueThreshold(64).
			WithLogger(nil)
	}
	if dir := os.Getenv(killWriterDirEnv); dir != "" {
		s, err := Open(opts(dir), false)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		value := bytes.Repeat([]byte{7}, 4<<10)
		for i := 0; ; i++ {
			tx, err := s.NewTransaction(ctx, true)
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Set(ctx, fmt.Appendf(nil, "key-%08d", i), value); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			fmt.Printf("committed %d\n", i)
		}
	}

	// Start the writer and read its reports until it has rotated files.
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestKilledWriterKeepsCommits$")
	cmd.Env = append(os.Environ(), killWriterDirEnv+"="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	last := -1
	lines := bufio.NewScanner(stdout)
	for last < 2000 && lines.Scan() {
		if _, err := fmt.Sscanf(lines.Text(), "committed %d", &last); err != nil {
			continue
		}
	}

	// Kill it mid-write.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	if last < 2000 {
		t.Fatalf("writer stopped at %d", last)
	}

	// Every reported commit reads back after the reopen.
	s, err := Open(opts(dir), false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	tx, err := s.NewTransaction(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Discard()
	for i := range last + 1 {
		value, found, err := tx.Get(ctx, fmt.Appendf(nil, "key-%08d", i))
		if err != nil || !found || len(value) != 4<<10 {
			t.Fatalf("key %d: found=%v len=%d err=%v", i, found, len(value), err)
		}
	}
}
