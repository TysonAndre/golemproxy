package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/TysonAndre/golemproxy/memcache"
)

// TODO: Refactor to use flag
var WORKERS = flag.Int("workers", 10, "number of clients to create to send sequential requests")
var LISTEN_ADDR = flag.String("server", "127.0.0.1:21211", "address of the server to connect to")

func benchmarker(total *int) {
	client := memcache.New(*LISTEN_ADDR, 1, 1*time.Second)
	item := memcache.Item{
		Key:   "key",
		Value: []byte("long long value"),
	}
	_ = item
	for {
		var err error
		/*
			err = client.Set(&item)
			if err != nil {
				panic(fmt.Sprintf("unexpected error: %v", err))
			}
		*/
		_, err = client.Get("key")
		if err != nil && err != memcache.ErrCacheMiss {
			panic(fmt.Sprintf("unexpected error: %v", err))
		}
		*total += 2
	}
}

func main() {
	// Must be called before using flags
	flag.Parse()

	totals := make([]int, *WORKERS)
	for i := 0; i < *WORKERS; i++ {
		go benchmarker(&totals[i])
	}
	c := time.Tick(time.Second)
	oldTotal := 0
	for range c {
		total := 0
		for _, v := range totals {
			total += v
		}
		fmt.Printf("Requests per second on %d workers: %d\n", *WORKERS, total-oldTotal)
		oldTotal = total
	}
}
