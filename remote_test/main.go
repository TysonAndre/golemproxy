package main

import (
	"github.com/TysonAndre/gomemcache/memcache"

	"fmt"
	"sync"
	"time"
)

var value = []byte("somelongvaluexxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

const ConcurrentRequests = 100

func setKeys(remote *memcache.Client) {
	var wg sync.WaitGroup
	wg.Add(ConcurrentRequests)

	//start := time.Now()

	for i := 0; i < ConcurrentRequests; i++ {
		go func(i int) {
			defer wg.Done()
			key := "tandre_test_key" + fmt.Sprintf("%d", i)
			err := remote.Set(&memcache.Item{
				Key:   key,
				Value: []byte(string(value) + fmt.Sprintf("%d", i)),
			})
			if err != nil {
				fmt.Printf("Saw error %d in set: %v\n", i, err)
				return
			}
			// elapsed := time.Since(start).Seconds()
			// fmt.Printf("Set %d %s %.6f\n", i, key, elapsed)
		}(i)
	}
	wg.Wait()
}

func getKeys(remote *memcache.Client) {
	var wg sync.WaitGroup
	wg.Add(ConcurrentRequests)

	for i := 0; i < ConcurrentRequests; i++ {
		go func(i int) {
			defer wg.Done()
			_, err := remote.Get("tandre_test_key" + fmt.Sprintf("%d", i))
			if err != nil {
				fmt.Printf("Saw error %d in get: %v\n", i, err)
				return
			}
			// fmt.Printf("Fetched %d %s\n", i, item.Value)
		}(i)
	}
	wg.Wait()
}

func runSetGet(remote *memcache.Client) {
	start := time.Now()
	setKeys(remote)
	middle := time.Now()
	fmt.Printf("Elapsed set %.6f", middle.Sub(start).Seconds())
	getKeys(remote)
	end := time.Now()
	fmt.Printf("Elapsed get %.6f", end.Sub(middle).Seconds())
}

func main() {
	//remote := memcache.New("smaincache01.tag-stage.com:11411")
	remote := memcache.New("smaincache01.tag-stage.com:11411")
	remote.Timeout = 1000 * time.Millisecond
	runSetGet(remote)
	runSetGet(remote)
	runSetGet(remote)
}
