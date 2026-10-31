//go:build darwin || linux || windows

package kvtx

import (
	"context"
	"testing"

	block_gc "github.com/s4wave/spacewave/db/block/gc"
	db_kvtx "github.com/s4wave/spacewave/db/kvtx"
	store_kvkey "github.com/s4wave/spacewave/db/store/kvkey"
	store_kvtx "github.com/s4wave/spacewave/db/store/kvtx"
	store_kvtx_inmem "github.com/s4wave/spacewave/db/store/kvtx/inmem"
)

// TestVolumeSharedCountsConfig opens one store of format 5 that tracks blocks
// under each Config in turn: without the upgrade flag it stays at format 5
// and keeps no counts; the flag moves it to format 6 and begins the walk;
// disabling the counts stops the walk; enabling them again begins a new one,
// which a block pass runs to the end. The conflicting pair is refused, and an
// empty store moves to format 6 at once without the flag.
func TestVolumeSharedCountsConfig(t *testing.T) {
	// Write a format 5 store whose graph tracks a block.
	ctx := t.Context()
	store := store_kvtx_inmem.NewStore()
	keys := store_kvkey.NewDefaultKVKey()
	tx, err := store.NewTransaction(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Set(ctx, keys.GetFormatVersionKey(), store_kvkey.MarshalFormatVersion(5)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := OpenRefGraph(store).AddRef(ctx, block_gc.BucketIRI("bucket"), testBlockIRI(t, "a")); err != nil {
		t.Fatal(err)
	}

	// open opens and closes a Volume over the store, checks its format, its
	// counts and its walk's phase, and runs fn while it is open.
	open := func(conf *store_kvtx.Config, format uint64, counts bool, phase block_gc.SharingPhase, fn func(*Volume)) {
		// Open the Volume.
		t.Helper()
		v, err := NewVolume(ctx, "test/sharing", keys, store, conf, false, false, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := v.Close(); err != nil {
				t.Error(err)
			}
		}()

		// Check its format, counts and walk.
		got, err := readFormatVersion(ctx, store, keys)
		if err != nil || got != format {
			t.Fatalf("format %d err %v, want %d", got, err, format)
		}
		if v.holdCounts != counts {
			t.Fatalf("hold counts %v, want %v", v.holdCounts, counts)
		}
		checkSharingPhase(t, store, phase)
		if fn != nil {
			fn(v)
		}
	}

	// Without the flag the store stays at format 5 and keeps no counts.
	open(&store_kvtx.Config{}, 5, false, block_gc.SharingComplete, nil)

	// The conflicting pair is refused.
	conflict := &store_kvtx.Config{DisableHoldCounts: true, UpgradeSharedCounts: true}
	if _, err := NewVolume(ctx, "test/sharing", keys, store, conflict, false, false, nil, nil); err == nil {
		t.Fatal("opened with disable_hold_counts and upgrade_shared_counts")
	}

	// The flag moves the store to format 6 and begins the walk.
	open(&store_kvtx.Config{UpgradeSharedCounts: true}, store_kvkey.FormatVersion, true, block_gc.SharingClearingHolds, nil)

	// Disabling the counts stops the walk.
	open(&store_kvtx.Config{DisableHoldCounts: true}, store_kvkey.FormatVersion, false, block_gc.SharingOff, nil)

	// Enabling them again begins a new walk, which a block pass completes.
	open(&store_kvtx.Config{}, store_kvkey.FormatVersion, true, block_gc.SharingClearingHolds, func(v *Volume) {
		if err := v.walkSharing(ctx); err != nil {
			t.Fatal(err)
		}
		checkSharingPhase(t, store, block_gc.SharingComplete)
	})

	// An empty store moves to format 6 at once and keeps counts.
	store = store_kvtx_inmem.NewStore()
	open(&store_kvtx.Config{}, store_kvkey.FormatVersion, true, block_gc.SharingComplete, nil)
}

// checkSharingPhase checks the phase of the sharing walk in store's graph.
func checkSharingPhase(t *testing.T, store db_kvtx.Store, phase block_gc.SharingPhase) {
	t.Helper()
	s, err := OpenRefGraph(store).GetSharing(context.Background())
	if err != nil || s.Phase != phase {
		t.Fatalf("sharing phase %c err %v, want %c", s.Phase, err, phase)
	}
}
