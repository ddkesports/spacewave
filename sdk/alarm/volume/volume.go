// Package alarm_volume defines the Alarms on store health: a volume stuck in
// one collection phase, a volume whose free disk will reach its reserve soon,
// and a volume that refuses writes at its reserve. Each reads the
// spacewave_volume_* metrics a Prometheus target scrapes from a Device, and is
// placed as an alarm/watch node.
package alarm_volume

import (
	"strconv"
	"time"

	alarm_watch "github.com/s4wave/spacewave/sdk/alarm/watch"
)

// Definitions returns the store health Alarms with the thresholds of opts.
func Definitions(opts Options) []*Definition {
	opts = opts.withDefaults()
	return []*Definition{
		GCStuck(opts.GCStuckAfter),
		ReserveSoon(opts.ReserveWithin, opts.ReserveSoonHold),
		Reserve(),
	}
}

// GCStuck fires while a volume has been in one collection phase other than
// idle for longer than after.
func GCStuck(after time.Duration) *Definition {
	return &Definition{
		Name:  "volume-gc-stuck",
		Title: "Volume collection stuck",
		Queries: []*alarm_watch.Query{{
			Name: "stuck",
			Expr: `(spacewave_volume_gc_phase{phase!="idle"} == 1) * ignoring(phase) group_left spacewave_volume_gc_phase_seconds > ` + seconds(after),
		}},
		Source: gcStuckSource,
	}
}

// ReserveSoon fires once a volume's free disk, falling at its rate over the
// last hour, will reach its reserve within the given time, and the forecast
// has held for hold.
func ReserveSoon(within, hold time.Duration) *Definition {
	return &Definition{
		Name:  "volume-reserve-soon",
		Title: "Volume disk filling",
		Hold:  hold,
		Queries: []*alarm_watch.Query{{
			Name: "hours",
			Expr: `clamp_min(spacewave_volume_free_disk_bytes - spacewave_volume_reserve_bytes, 0) / (-deriv(spacewave_volume_free_disk_bytes[1h]) > 0) / 3600 < ` + strconv.FormatFloat(within.Hours(), 'g', -1, 64),
		}},
		Source: reserveSoonSource,
	}
}

// Reserve fires while a volume refuses new writes because its free disk is
// below its reserve.
func Reserve() *Definition {
	return &Definition{
		Name:  "volume-reserve",
		Title: "Volume refusing writes",
		Queries: []*alarm_watch.Query{{
			Name: "reserve",
			Expr: `spacewave_volume_write_reserve == 1`,
		}},
		Source: reserveSource,
	}
}

// seconds formats d in seconds for a PromQL comparison.
func seconds(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'g', -1, 64)
}
