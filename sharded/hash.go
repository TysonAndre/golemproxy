package sharded

import (
	"fmt"
	"hash/fnv"
)

// compatible with twemproxy's fnv64a implementation
func fnv64a(key []byte) uint32 {
	// compute the 64-bit fnv64a and take the lower 32 bits for hashing
	hasher := fnv.New64a()
	hasher.Write(key)
	return uint32(hasher.Sum64())
}

func createHasher(algorithm string) func(key []byte) uint32 {
	switch algorithm {
	case "fnv1a_64":
		return fnv64a
	default:
		panic(fmt.Sprintf("unknown hash algorithm %q", algorithm))
	}
}
