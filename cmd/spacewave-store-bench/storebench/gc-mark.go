package storebench

import (
	"context"

	"github.com/pkg/errors"
	block_gc "github.com/s4wave/spacewave/db/block/gc"
	volume_kvtx "github.com/s4wave/spacewave/db/volume/common/kvtx"
)

// collectLoop runs block GC passes on the account volume back to back until
// ctx ends, so every write of the window overlaps a mark, and logs each pass.
func (l *load) collectLoop(ctx context.Context) error {
	// Collect through the volume's ref graph, as the account cleanup does.
	vol, ok := l.vol.(*volume_kvtx.Volume)
	if !ok || vol.GetRefGraph() == nil {
		return errors.New("gc mark: the account volume has no ref graph")
	}
	collector := block_gc.NewCollector(vol.GetRefGraph(), vol, nil)

	// Run passes until the load stops, ignoring the error of the cut pass.
	for passes := 1; ; passes++ {
		stats, err := collector.Collect(ctx)
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
			WithField("removed-blocks", stats.RemoveBlockCount).
			Info("gc pass")
	}
}
