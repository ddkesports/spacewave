package storebench

import (
	"context"
	"time"

	"github.com/pkg/errors"
	block_gc "github.com/s4wave/spacewave/db/block/gc"
	volume_kvtx "github.com/s4wave/spacewave/db/volume/common/kvtx"
)

// minorBudget is the budget of each minor pass's call, which bounds the
// sweep of unreferenced nodes before the minor pass runs.
const minorBudget = time.Second

// collectLoop runs block GC passes on the account volume back to back until
// ctx ends, so every write of the window overlaps a mark, and logs each pass.
// With GCMinor, every pass after the first is a minor pass.
func (l *load) collectLoop(ctx context.Context) error {
	// Collect through the volume's ref graph, as the account cleanup does.
	vol, ok := l.vol.(*volume_kvtx.Volume)
	if !ok || vol.GetRefGraph() == nil {
		return errors.New("gc mark: the account volume has no ref graph")
	}
	collector := block_gc.NewCollector(vol.GetRefGraph(), vol, nil)

	// Run passes until the load stops, ignoring the error of the cut pass.
	// After a first block pass, a minor run defers the next block pass past
	// the window, so each budgeted call runs a minor pass.
	for passes := 1; ; passes++ {
		var stats *block_gc.Stats
		var err error
		if l.conf.GCMinor && passes > 1 {
			stats, err = collector.CollectFor(ctx, minorBudget)
		} else {
			stats, err = collector.Collect(ctx)
		}
		if l.conf.GCMinor && passes == 1 {
			collector.SetMarkInterval(24*time.Hour, time.Time{})
		}
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return errors.Wrap(err, "gc mark")
		}
		l.le.WithField("pass", passes).
			WithField("duration", stats.Duration).
			WithField("mark-duration", stats.MarkDuration).
			WithField("mark-scans", stats.MarkScans).
			WithField("mark-longest-read", stats.MarkLongestRead).
			WithField("minor-passes", stats.MinorPasses).
			WithField("young-blocks", stats.YoungBlocks).
			WithField("removed-blocks", stats.RemoveBlockCount).
			Info("gc pass")
	}
}
