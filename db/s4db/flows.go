package s4db

import (
	"bytes"
	"sync/atomic"
	"time"
)

// Flow is the length of the values commits stored outside the index under
// one key family, and of the stored values they replaced or deleted there,
// since the database opened.
type Flow struct {
	// Written is the length of the values commits stored.
	Written int64
	// Freed is the length of the stored values commits replaced or deleted.
	Freed int64
}

// flowCount accumulates one family's Flow.
type flowCount struct {
	// written and freed are the Flow's lengths.
	written, freed atomic.Int64
}

// Flows returns the flow of each of Options.Families in order, then of the
// keys under none. It counts this handle's commits, but not compaction's
// moves, which free what they write, nor other processes' commits.
func (db *DB) Flows() []Flow {
	out := make([]Flow, len(db.flows))
	for i := range db.flows {
		out[i] = Flow{Written: db.flows[i].written.Load(), Freed: db.flows[i].freed.Load()}
	}
	return out
}

// BuildWait returns the time this handle's background commits waited for a
// checkpoint build, since the database opened.
func (db *DB) BuildWait() time.Duration {
	return time.Duration(db.w.stats.buildWaitNanos.Load())
}

// family returns the index in flows of the family key starts with.
func (db *DB) family(key []byte) int {
	for i, prefix := range db.opts.Families {
		if bytes.HasPrefix(key, prefix) {
			return i
		}
	}
	return len(db.opts.Families)
}

// addFlows adds a commit's flows to the database's.
func (db *DB) addFlows(flows []Flow) {
	for i, f := range flows {
		if f.Written != 0 {
			db.flows[i].written.Add(f.Written)
		}
		if f.Freed != 0 {
			db.flows[i].freed.Add(f.Freed)
		}
	}
}
