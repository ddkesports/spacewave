//go:build darwin || linux || windows

package s4db

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestSmallCommitSkipsBuildWait holds a background checkpoint while a bulk
// commit pushes the overlay past its bound, and checks that a small commit
// returns while the bulk commit waits for the build.
func TestSmallCommitSkipsBuildWait(t *testing.T) {
	// Open a file with small checkpoints whose builds wait for the gate.
	ctx := context.Background()
	opts := Options{CheckpointMin: 16 << 10, CheckpointMax: 64 << 10}
	db := openTest(t, filepath.Join(t.TempDir(), "wait.s4wave"), opts)
	defer db.Close()
	started, gate := make(chan struct{}, 1), make(chan struct{})
	db.w.beforeBuild = func() {
		select {
		case started <- struct{}{}:
		default:
		}
		<-gate
	}
	released := false
	defer func() {
		if !released {
			close(gate)
		}
	}()

	// Commit a bulk batch over twice CheckpointMax, which starts the build
	// and waits for it.
	bulk := make(chan error, 1)
	go func() {
		// Write 800 keys of 100 bytes in one transaction.
		tx, err := db.NewTransaction(ctx, true)
		if err != nil {
			bulk <- err
			return
		}
		val := make([]byte, 100)
		for i := range 800 {
			if err := tx.Set(ctx, fmt.Appendf(nil, "bulk/%06d", i), val); err != nil {
				bulk <- err
				return
			}
		}
		bulk <- tx.Commit(ctx)
	}()
	select {
	case <-started:
	case err := <-bulk:
		t.Fatalf("bulk commit returned %v before the build started", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the build did not start")
	}
	if got, limit := db.cur.Load().overlayBytes, 2*opts.CheckpointMax; got <= limit || got > smallLimit*limit {
		t.Fatalf("overlay holds %d bytes, want over %d and at most %d", got, limit, smallLimit*limit)
	}

	// A small commit returns while the build holds.
	small := make(chan error, 1)
	go func() {
		tx, err := db.NewTransaction(ctx, true)
		if err != nil {
			small <- err
			return
		}
		if err := tx.Set(ctx, []byte("small"), []byte("v")); err != nil {
			small <- err
			return
		}
		small <- tx.Commit(ctx)
	}()
	select {
	case err := <-small:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("small commit waited for the build")
	}

	// The bulk commit still waits, and returns once the build ends.
	select {
	case err := <-bulk:
		t.Fatalf("bulk commit returned %v while the build held", err)
	case <-time.After(100 * time.Millisecond):
	}
	released = true
	close(gate)
	select {
	case err := <-bulk:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bulk commit did not return after the build")
	}

	// The stats count the wait and, once it ends, the checkpoint it waited
	// for.
	db.w.builds.Wait()
	stats := db.Stats()
	if stats.BuildWaits != 1 || stats.BuildWaitTime < 100*time.Millisecond {
		t.Fatalf("build waits %d for %v, want 1 for at least 100ms", stats.BuildWaits, stats.BuildWaitTime)
	}
	if stats.Checkpoints < 1 || stats.CheckpointBytes == 0 || stats.CheckpointTime < 100*time.Millisecond {
		t.Fatalf("checkpoints %d of %d bytes in %v", stats.Checkpoints, stats.CheckpointBytes, stats.CheckpointTime)
	}

	// The small commit's key reads back.
	rtx, err := db.NewTransaction(ctx, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Discard()
	if v, ok, err := rtx.Get(ctx, []byte("small")); err != nil || !ok || string(v) != "v" {
		t.Fatalf("get small = %q %v %v", v, ok, err)
	}
}

// TestCommitWaitMaxRaisesBound holds a background checkpoint and checks that
// a bulk commit past twice CheckpointMax returns while a larger CommitWaitMax
// still covers the overlay.
func TestCommitWaitMaxRaisesBound(t *testing.T) {
	// Open a file with small checkpoints, a large commit wait bound, and
	// builds that wait for the gate.
	ctx := context.Background()
	opts := Options{CheckpointMin: 16 << 10, CheckpointMax: 64 << 10, CommitWaitMax: 8 << 20}
	db := openTest(t, filepath.Join(t.TempDir(), "wait.s4wave"), opts)
	defer db.Close()
	started, gate := make(chan struct{}, 1), make(chan struct{})
	defer close(gate)
	db.w.beforeBuild = func() {
		select {
		case started <- struct{}{}:
		default:
		}
		<-gate
	}

	// Commit a bulk batch over twice CheckpointMax while the build holds.
	tx, err := db.NewTransaction(ctx, true)
	if err != nil {
		t.Fatal(err)
	}
	val := make([]byte, 100)
	for i := range 800 {
		if err := tx.Set(ctx, fmt.Appendf(nil, "bulk/%06d", i), val); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- tx.Commit(ctx) }()

	// It returns without waiting for the build it started.
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the build did not start")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bulk commit waited for the build")
	}
	if got := db.cur.Load().overlayBytes; got <= 2*opts.CheckpointMax {
		t.Fatalf("overlay holds %d bytes, want over %d", got, 2*opts.CheckpointMax)
	}
	if stats := db.Stats(); stats.BuildWaits != 0 {
		t.Fatalf("build waits %d, want 0", stats.BuildWaits)
	}
}
