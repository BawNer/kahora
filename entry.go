package kahora

// entry is the internal representation of a cached value.
// expiresAt uses monoNow() — monotonic nanoseconds since process start.
// Never store time.Time here — we optimise for size and cache-line friendliness.
type entry[V any] struct {
	value     V
	expiresAt int64 // monoNow() + ttl; 0 = no TTL
}

// isExpired reports whether the entry has passed its TTL.
// Always returns false when TTL is not set (expiresAt == 0).
func (e *entry[V]) isExpired(now int64) bool {
	return e.expiresAt != 0 && now >= e.expiresAt
}
