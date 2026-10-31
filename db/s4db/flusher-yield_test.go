package s4db

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestYieldWaitsForRunningFlush checks that a checkpoint batch waits for a
// running shared flush to end and passes at once when none runs.
func TestYieldWaitsForRunningFlush(t *testing.T) {
	// No flush runs: yield returns at once.
	f := newFlusher(nil, &atomic.Pointer[state]{})
	f.yield()

	// Mark a flush running and yield behind it.
	l := f.bcast.Lock()
	f.flushing = true
	l.Unlock()
	done := make(chan struct{})
	go func() {
		f.yield()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("yield passed a running flush")
	case <-time.After(50 * time.Millisecond):
	}

	// End the flush: yield returns.
	l = f.bcast.Lock()
	f.flushing = false
	l.Broadcast()
	l.Unlock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("yield did not return after the flush ended")
	}
}
