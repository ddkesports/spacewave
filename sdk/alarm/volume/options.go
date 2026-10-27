package alarm_volume

import "time"

// Options are the thresholds of the store health Alarms. A zero field takes
// its default.
type Options struct {
	// GCStuckAfter is how long a volume may stay in one collection phase
	// other than idle. Default 30 minutes.
	GCStuckAfter time.Duration
	// ReserveWithin is how soon free disk may be forecast to reach the
	// reserve before the Alarm holds. Default 24 hours.
	ReserveWithin time.Duration
	// ReserveSoonHold is how long that forecast must hold before the Alarm
	// fires, so a burst of writes does not fire it. Default 15 minutes.
	ReserveSoonHold time.Duration
}

// withDefaults returns opts with each zero field set to its default.
func (o Options) withDefaults() Options {
	if o.GCStuckAfter <= 0 {
		o.GCStuckAfter = 30 * time.Minute
	}
	if o.ReserveWithin <= 0 {
		o.ReserveWithin = 24 * time.Hour
	}
	if o.ReserveSoonHold <= 0 {
		o.ReserveSoonHold = 15 * time.Minute
	}
	return o
}
