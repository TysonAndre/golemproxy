package sharded

import (
	"fmt"

	"github.com/TysonAndre/golemproxy/memcache"
	"github.com/TysonAndre/golemproxy/memcache/sharded/ketama"
)

func createKetamaDistribution(clients []*memcache.PipeliningClient) func(h uint32) int {
	buckets := make([]ketama.Bucket, len(clients))
	for i, client := range clients {
		buckets[i] = ketama.Bucket{
			Label:  client.Label,
			Weight: client.Weight,
			Data:   i,
		}
	}

	ketama, err := ketama.NewKetama(buckets)
	if err != nil {
		panic("Failed to create ketama distribution")
	}
	return func(h uint32) int {
		return ketama.Get(h)
	}
}

func createDistribution(distribution string, clients []*memcache.PipeliningClient) func(h uint32) int {
	switch distribution {
	case "ketama":
		return createKetamaDistribution(clients)
	default:
		panic(fmt.Sprintf("unknown distribution %q", distribution))
	}
}
