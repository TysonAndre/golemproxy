package memcache

import (
	"errors"
	"github.com/TysonAndre/golemproxy/config"

	"fmt"
	"sync"
)

type ShardedClient struct {
	getClient func(key string) *PipeliningClient
	clients   []*PipeliningClient
}

var _ ClientInterface = &ShardedClient{}

func (c *ShardedClient) Get(key string) (item *Item, err error) {
	return c.getClient(key).Get(key)
}

func (c *ShardedClient) GetMulti(keys []string) (map[string]*Item, error) {
	if len(keys) == 1 {
		return c.getClient(keys[0]).GetMulti(keys)
	} else if len(keys) == 0 {
		return make(map[string]*Item, 0), nil
	}
	results := make(map[string]*Item)
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

func (c *ShardedClient) GetMultiArray(keys []string) ([]*Item, error) {
	if len(keys) == 1 {
		return c.getClient(keys[0]).GetMultiArray(keys)
	} else if len(keys) == 0 {
		return nil, nil
	}
	var results []*Item
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

func (c *ShardedClient) Set(item *Item) error {
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

func (c *ShardedClient) Add(item *Item) error {
	return c.getClient(item.Key).Add(item)
}

func (c *ShardedClient) Replace(item *Item) error {
	return c.getClient(item.Key).Replace(item)
}

func (c *ShardedClient) Increment(key string, delta uint64) (newValue uint64, err error) {
	return c.getClient(key).Increment(key, delta)
}

func (c *ShardedClient) Decrement(key string, delta uint64) (newValue uint64, err error) {
	return c.getClient(key).Decrement(key, delta)
}

func FromConfig(conf config.Config) ClientInterface {
	servers := conf.Servers
	if len(servers) == 0 {
		panic("Expected 1 or more servers")
	}

	clients := []*PipeliningClient{}
	for _, serverConfig := range servers {
		connString := fmt.Sprintf("%s:%d", serverConfig.Host, serverConfig.Port)
		clients = append(clients, New(connString))
	}
	if len(clients) == 1 {
		return clients[0]
	}
	return &ShardedClient{
		getClient: func(key string) *PipeliningClient {
			// FIXME: implement or reuse hash ring algorithm
			return clients[0]
		},
		clients: clients,
	}
}
