package storebench

import (
	"slices"
	"time"
)

// summarize returns the percentiles of durs. It sorts durs in place.
func summarize(durs []time.Duration) *Latency {
	// Report an empty set as zero.
	out := &Latency{Count: uint64(len(durs))}
	if len(durs) == 0 {
		return out
	}

	// Sort, then read the ranks and sum the mean.
	slices.Sort(durs)
	var sum time.Duration
	for _, d := range durs {
		sum += d
	}
	out.P50Us = micros(durs[rank(len(durs), 0.50)])
	out.P99Us = micros(durs[rank(len(durs), 0.99)])
	out.MaxUs = micros(durs[len(durs)-1])
	out.MeanUs = micros(sum / time.Duration(len(durs)))
	return out
}

// rank returns the nearest rank index of quantile q in a sorted set of n.
func rank(n int, q float64) int {
	i := int(q*float64(n)+0.999999) - 1
	return min(max(i, 0), n-1)
}

// micros returns d in whole microseconds.
func micros(d time.Duration) uint64 {
	return uint64(d / time.Microsecond)
}
