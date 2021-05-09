package sharded

import (
	"fmt"

	"github.com/TysonAndre/golemproxy/memcache"
	"github.com/TysonAndre/golemproxy/sharded/distribution"
)

func createKetamaDistribution(buckets []distribution.Bucket) func(h uint32) int {
	ketama, err := distribution.NewKetama(buckets)
	if err != nil {
		panic("Failed to create ketama distribution")
	}
	return func(h uint32) int {
		return ketama.Get(h)
	}
}

func createModulaDistribution(buckets []distribution.Bucket) func(h uint32) int {
	ketama, err := distribution.NewModula(buckets)
	if err != nil {
		panic("Failed to create ketama distribution")
	}
	return func(h uint32) int {
		return ketama.Get(h)
	}
}

func createDistribution(distributionType string, clients []*memcache.PipeliningClient) func(h uint32) int {
	if len(clients) == 0 {
		panic("Expected 1 or more clients when creating distribution")
	}
	buckets := make([]distribution.Bucket, len(clients))
	for i, client := range clients {
		buckets[i] = distribution.Bucket{
			Label:  client.Label,
			Weight: client.Weight,
			Data:   i,
		}
	}

	switch distributionType {
	case "ketama":
		return createKetamaDistribution(buckets)
	case "modula":
		return createModulaDistribution(buckets)
	default:
		panic(fmt.Sprintf("unknown distribution %q", distributionType))
	}
}
