package kahora

import "errors"

// ErrCapacityExceeded is returned by Set when the cache has reached its
// maxEntries limit. The limit is per-shard and approximate — see WithMaxEntries.
var ErrCapacityExceeded = errors.New("kahora: capacity exceeded")

// ErrClosed is returned by Set when called on a closed cache.
// Get and Delete remain safe after Close but no further writes are accepted.
var ErrClosed = errors.New("kahora: cache is closed")
