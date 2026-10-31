package s4db

import (
	"sync/atomic"
	"time"
)

// Stats reports the checkpoint work of a handle and the time its writers
// waited for it, since the handle opened.
type Stats struct {
	// Checkpoints is the number of background checkpoints that ended.
	Checkpoints uint64
	// CheckpointTime is the time the ended background checkpoints ran, from
	// the start of the build to the release of the pages they replaced.
	CheckpointTime time.Duration
	// CheckpointBytes is the size of the pages the background checkpoints
	// wrote.
	CheckpointBytes uint64
	// Building reports a background checkpoint running now.
	Building bool
	// BuildWaits is the number of commits that waited for a background
	// checkpoint because they outpaced it.
	BuildWaits uint64
	// BuildWaitTime is the time those commits waited.
	BuildWaitTime time.Duration
	// Flushes is the number of flushes to the drive that commits shared.
	Flushes uint64
	// FlushTime is the time those flushes took. Durable commits wait for
	// them, so it bounds the commit latency the drive adds.
	FlushTime time.Duration
	// OverlayBytes is the size of the changes since the last checkpoint,
	// which the next one writes into the index.
	OverlayBytes int64
	// LogBytes is the size of the log since the last checkpoint.
	LogBytes int64
}

// writerStats counts a writer's checkpoint work. Its fields are safe without
// the writer lock.
type writerStats struct {
	checkpoints, checkpointBytes, buildWaits atomic.Uint64
	checkpointNanos, buildWaitNanos          atomic.Int64
}

// Stats returns the handle's checkpoint work since it opened.
func (db *DB) Stats() Stats {
	w, st := db.w, db.cur.Load()
	return Stats{
		Checkpoints:     w.stats.checkpoints.Load(),
		CheckpointTime:  time.Duration(w.stats.checkpointNanos.Load()),
		CheckpointBytes: w.stats.checkpointBytes.Load(),
		Building:        w.building.Load(),
		BuildWaits:      w.stats.buildWaits.Load(),
		BuildWaitTime:   time.Duration(w.stats.buildWaitNanos.Load()),
		Flushes:         db.flush.flushes.Load(),
		FlushTime:       time.Duration(db.flush.flushNanos.Load()),
		OverlayBytes:    st.overlayBytes,
		LogBytes:        st.logBytes,
	}
}
