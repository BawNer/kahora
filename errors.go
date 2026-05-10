package kahora

import "errors"

var (
	// ErrCapacityExceeded is returned by Set under EvictionReject when the
	// shard is at its share of maxEntries.
	ErrCapacityExceeded = errors.New("kahora: capacity exceeded")

	// ErrClosed is returned by Set after Close.
	ErrClosed = errors.New("kahora: cache is closed")
)
