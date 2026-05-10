package kahora

type entry[V any] struct {
	value     V
	expiresAt int64 // monoNow() + ttl; 0 = no TTL
}

func (e *entry[V]) isExpired(now int64) bool {
	return e.expiresAt != 0 && now >= e.expiresAt
}
