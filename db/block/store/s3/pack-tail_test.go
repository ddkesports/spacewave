package block_store_s3

import (
	"bytes"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
)

// TestPackStoreMergesTail writes short batches into a store that keeps its
// packs and merges its tail, while a second store reads the packfiles the
// merges delete. Every block stays readable through each merge, a full tail
// starts a new packfile, and an unreadable tail is kept rather than replaced.
func TestPackStoreMergesTail(t *testing.T) {
	ctx := t.Context()
	bucket, client := newFakeBucket(t, nil)
	opts := &PackStoreOptions{KeepPacks: true, MergeTailBytes: 4 << 10}
	store := NewPackStore(logrus.NewEntry(logrus.New()), client, "bucket", "p/", opts)
	t.Cleanup(store.Close)
	packs := func() []string {
		bucket.mtx.Lock()
		defer bucket.mtx.Unlock()
		var keys []string
		for key := range bucket.objects {
			if strings.HasPrefix(key, "p/"+packDir) {
				keys = append(keys, key)
			}
		}
		return keys
	}

	// A reader lists the first packfile, which every later merge replaces.
	refs := putBlocks(t, store, "block", 1)
	reader := newTestPackStore(t, client)
	checkBlocks(t, reader, "block", refs)

	// Forty one-block batches, and one repeated block, merge into one packfile.
	// The reader finds the packfile it listed deleted, lists again, and reads
	// the block from the merged packfile.
	refs = putBlocks(t, store, "block", 40)
	if got := packs(); len(got) != 1 {
		t.Fatalf("bucket holds %d packfiles after short batches; want 1", len(got))
	}
	checkBlocks(t, reader, "block", refs[:1])

	// A block that fills the tail past the merge size merges once, and the next
	// short batch starts a new packfile.
	if _, _, err := store.PutBlock(ctx, bytes.Repeat([]byte{'x'}, 5<<10), nil); err != nil {
		t.Fatal(err)
	}
	if got := packs(); len(got) != 1 {
		t.Fatalf("bucket holds %d packfiles after filling the tail; want 1", len(got))
	}
	more := putBlocks(t, store, "more", 1)
	tails := packs()
	if len(tails) != 2 {
		t.Fatalf("bucket holds %d packfiles after a full tail; want 2", len(tails))
	}

	// Corrupt the new tail: the next batch is written beside it, and the
	// unreadable tail is kept.
	tail := "p/" + packDir + store.pickTail(packPayloadBytes).GetId()
	bucket.mtx.Lock()
	good := bucket.objects[tail]
	bucket.objects[tail] = "not a packfile"
	bucket.mtx.Unlock()
	late := putBlocks(t, store, "late", 1)
	if got := packs(); len(got) != 3 {
		t.Fatalf("bucket holds %d packfiles after a corrupt tail; want 3", len(got))
	}
	bucket.mtx.Lock()
	bucket.objects[tail] = good
	bucket.mtx.Unlock()

	// A fresh store reads every block from the packfiles that remain.
	fresh := newTestPackStore(t, client)
	checkBlocks(t, fresh, "block", refs)
	checkBlocks(t, fresh, "more", more)
	checkBlocks(t, fresh, "late", late)
}
