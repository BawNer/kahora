package kahora

import "time"

// processStart is the reference point for all monotonic time calculations.
// Initialized once at package load. All monoNow() values are relative to this.
//
// We use time.Since(processStart) internally, which uses Go's monotonic clock
// component — immune to NTP adjustments, leap seconds, or manual time changes.
//
// Note: values returned by monoNow() are not wall clock timestamps.
// They are only meaningful when compared against other monoNow() values
// within the same process lifetime.
var processStart = time.Now()

// monoNow returns the number of nanoseconds elapsed since process start,
// using the monotonic clock. Safe to use for TTL and delta merge comparisons.
//
// Not suitable for logging human-readable timestamps — use time.Now() for that.
func monoNow() int64 {
	return time.Since(processStart).Nanoseconds()
}
