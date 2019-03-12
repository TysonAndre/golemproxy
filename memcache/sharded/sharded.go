// package sharded implements a memcache client that shards 2 or more memcache servers
package sharded

import (
	"errors"
	"github.com/TysonAndre/golemproxy/config"
	"github.com/TysonAndre/golemproxy/memcache"
	"os"

	"fmt"
	"sync"
)

type ShardedClient struct {
	getClient func(key string) *memcache.PipeliningClient
	clients   []*memcache.PipeliningClient
}

var _ memcache.ClientInterface = &ShardedClient{}

func (c *ShardedClient) Get(key string) (item *memcache.Item, err error) {
	return c.getClient(key).Get(key)
}

func (c *ShardedClient) GetMulti(keys []string) (map[string]*memcache.Item, error) {
	if len(keys) == 1 {
		return c.getClient(keys[0]).GetMulti(keys)
	} else if len(keys) == 0 {
		return make(map[string]*memcache.Item, 0), nil
	}
	results := make(map[string]*memcache.Item)
	var wg sync.WaitGroup
	var m sync.Mutex
	wg.Add(len(keys))
	var sharedErr error
	for _, key := range keys {
		go func(key string) {
			defer wg.Done()
			item, err := c.Get(key)
			m.Lock()
			defer m.Unlock()
			if err != nil {
				sharedErr = err
			}
			if item != nil {
				results[key] = item
			}
		}(key)
	}
	return results, sharedErr
}

func (c *ShardedClient) GetMultiArray(keys []string) ([]*memcache.Item, error) {
	if len(keys) == 1 {
		return c.getClient(keys[0]).GetMultiArray(keys)
	} else if len(keys) == 0 {
		return nil, nil
	}
	var results []*memcache.Item
	var wg sync.WaitGroup
	var m sync.Mutex
	wg.Add(len(keys))
	var sharedErr error
	for _, key := range keys {
		go func(key string) {
			defer wg.Done()
			item, err := c.Get(key)
			m.Lock()
			defer m.Unlock()
			if err != nil {
				sharedErr = err
			}
			if item != nil {
				results = append(results, item)
			}
		}(key)
	}
	return results, sharedErr
}

func (c *ShardedClient) Set(item *memcache.Item) error {
	return c.getClient(item.Key).Set(item)
}

func (c *ShardedClient) Delete(key string) error {
	return c.getClient(key).Delete(key)
}

func (c *ShardedClient) DeleteAll() error {
	return errors.New("Refusing to flush_all")
}

func (c *ShardedClient) Touch(key string, seconds int32) error {
	return c.getClient(key).Touch(key, seconds)
}

func (c *ShardedClient) Add(item *memcache.Item) error {
	return c.getClient(item.Key).Add(item)
}

func (c *ShardedClient) Replace(item *memcache.Item) error {
	return c.getClient(item.Key).Replace(item)
}

func (c *ShardedClient) Increment(key string, delta uint64) (newValue uint64, err error) {
	return c.getClient(key).Increment(key, delta)
}

func (c *ShardedClient) Decrement(key string, delta uint64) (newValue uint64, err error) {
	return c.getClient(key).Decrement(key, delta)
}

func New(conf config.Config) memcache.ClientInterface {
	servers := conf.Servers
	if len(servers) == 0 {
		panic("Expected 1 or more servers")
	}

	clients := []*memcache.PipeliningClient{}
	for _, serverConfig := range servers {
		connString := fmt.Sprintf("%s:%d", serverConfig.Host, serverConfig.Port)
		clients = append(clients, memcache.New(connString))
	}
	if len(clients) == 1 {
		return clients[0]
	}
	var hasher = createHasher(conf.Hash)

	return &ShardedClient{
		getClient: func(key string) *memcache.PipeliningClient {
			// FIXME: implement or reuse hash ring algorithm
			hash := hasher(key)
			fmt.Fprintf(os.Stderr, "Hash of %q is %d", key, hash)
			return clients[0]
		},
		clients: clients,
	}
}
