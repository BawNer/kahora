package kahora

import "time"

// processStart anchors the monotonic clock used for TTL.
// time.Since uses Go's monotonic component — NTP and wall clock jumps
// don't affect it.
var processStart = time.Now()

func monoNow() int64 {
	return time.Since(processStart).Nanoseconds()
}
