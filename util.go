package kahora

func isPowerOfTwo(n ShardCount) bool {
	return n > 0 && (n&(n-1)) == 0
}
