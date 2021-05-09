// package sharded implements a memcache client that shards 2 or more memcache servers
package sharded

import (
	"errors"
	"time"

	"github.com/TysonAndre/golemproxy/config"
	"github.com/TysonAndre/golemproxy/memcache"
	"github.com/TysonAndre/golemproxy/memcache/proxy/message"

	"fmt"
	"sync"
)

type ShardedClient struct {
	getClient func(key []byte) *memcache.PipeliningClient
	clients   []*memcache.PipeliningClient
}

var _ memcache.ClientInterface = &ShardedClient{}

func (c *ShardedClient) SendProxiedMessageAsync(command *message.SingleMessage) {
	// TODO: optimize out the string copy
	c.getClient(command.Key).SendProxiedMessageAsync(command)
}

func (c *ShardedClient) Get(key string) (item *memcache.Item, err error) {
	return c.getClient([]byte(key)).Get(key)
}

func (c *ShardedClient) GetMulti(keys []string) (map[string]*memcache.Item, error) {
	if len(keys) == 1 {
		return c.getClient([]byte(keys[0])).GetMulti(keys)
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
		return c.getClient([]byte(keys[0])).GetMultiArray(keys)
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
	return c.getClient([]byte(item.Key)).Set(item)
}

func (c *ShardedClient) Delete(key string) error {
	return c.getClient([]byte(key)).Delete(key)
}

func (c *ShardedClient) DeleteAll() error {
	return errors.New("Refusing to flush_all")
}

func (c *ShardedClient) Touch(key string, seconds int32) error {
	return c.getClient([]byte(key)).Touch(key, seconds)
}

func (c *ShardedClient) Add(item *memcache.Item) error {
	return c.getClient([]byte(item.Key)).Add(item)
}

func (c *ShardedClient) Replace(item *memcache.Item) error {
	return c.getClient([]byte(item.Key)).Replace(item)
}

func (c *ShardedClient) Increment(key string, delta uint64) (newValue uint64, err error) {
	return c.getClient([]byte(key)).Increment(key, delta)
}

func (c *ShardedClient) Decrement(key string, delta uint64) (newValue uint64, err error) {
	return c.getClient([]byte(key)).Decrement(key, delta)
}

func (c *ShardedClient) Finalize() {
	clients := c.clients
	if len(clients) == 0 {
		return
	}
	c.clients = nil

	var wg sync.WaitGroup
	wg.Add(len(clients))
	for _, server := range c.clients {
		go func() {
			server.Finalize()
			wg.Done()
		}()
	}
	wg.Wait()
}

func New(conf config.Config) memcache.ClientInterface {
	servers := conf.Servers
	if len(servers) == 0 {
		panic("Expected 1 or more servers")
	}

	clients := []*memcache.PipeliningClient{}
	for _, serverConfig := range servers {
		connString := fmt.Sprintf("%s:%d", serverConfig.Host, serverConfig.Port)
		client := memcache.New(connString, int(conf.MaxServerConnections), time.Duration(conf.Timeout)*time.Millisecond)
		client.Weight = int(serverConfig.Weight)
		client.Label = serverConfig.Key
		if client.Weight < 1 {
			panic("Expected positive weight")
		}
		clients = append(clients, client)
	}

	unique := make(map[string]*memcache.PipeliningClient)
	for _, client := range clients {
		unique[client.Label] = client
	}
	if len(unique) != len(servers) {
		panic(fmt.Sprintf("List of server labels is not unique: %#v", unique))
	}

	if len(clients) == 1 {
		return clients[0]
	}
	hasher := createHasher(conf.Hash)
	distribution := createDistribution(conf.Distribution, clients)

	return &ShardedClient{
		getClient: func(key []byte) *memcache.PipeliningClient {
			// FIXME: implement or reuse hash ring algorithm
			hash := hasher(key)
			clientIdx := distribution(hash)
			// fmt.Fprintf(os.Stderr, "Hash of %q is %d, clientIdx = %d\n", key, hash, clientIdx)
			return clients[clientIdx]
		},
		clients: clients,
	}
}
