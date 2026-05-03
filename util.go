package kahora

// isPowerOfTwo reports whether n is a positive power of two.
// Used to validate ShardCount — shardFor relies on bitmask instead of modulo.
func isPowerOfTwo(n ShardCount) bool {
	return n > 0 && (n&(n-1)) == 0
}
