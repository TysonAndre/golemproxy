/*

Copyright 2019 Tyson Andre

---

Based on https://github.com/bradfitz/gomemcache/blob/master/memcache/memcache.go
with many modifications

Copyright 2011 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package memcache provides a client for the memcached cache server.
package memcache

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"

	"strconv"
	"strings"
	"sync"
	"time"
)

// Similar to:
// http://code.google.com/appengine/docs/go/memcache/reference.html

var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.
	ErrCacheMiss = errors.New("memcache: cache miss")

	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.
	ErrNotStored = errors.New("memcache: item not stored")

	// ErrServer means that a server error occurred.
	ErrServerError = errors.New("memcache: server error")

	// ErrNoStats means that no statistics were available.
	ErrNoStats = errors.New("memcache: no statistics available")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long and not
	// contain whitespace or control characters.
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")

	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("memcache: no servers configured or available")
)

const (
	// DefaultTimeout is the default socket read/write timeout.
	DefaultTimeout = 100 * time.Millisecond

	// DefaultMaxIdleConns is the default maximum number of idle connections
	// kept for any single address.
	DefaultMaxIdleConns = 2
)

// resumableError returns true if err is only a protocol-level cache error.
// This is used to determine whether or not a server connection should
// be re-used or not. If an error occurs, by default we don't reuse the
// connection, unless it was just a cache error.
func resumableError(err error) bool {
	switch err {
	case ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrMalformedKey:
		return true
	}
	return false
}

func legalKey(key string) bool {
	if len(key) > 250 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] <= ' ' || key[i] == 0x7f {
			return false
		}
	}
	return true
}

var (
	crlf            = []byte("\r\n")
	space           = []byte(" ")
	resultOK        = []byte("OK\r\n")
	resultStored    = []byte("STORED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultExists    = []byte("EXISTS\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")
	resultTouched   = []byte("TOUCHED\r\n")

	resultClientErrorPrefix = []byte("CLIENT_ERROR ")

	valuePrefix = []byte("VALUE ")
)

// New returns a memcache client using the provided server.
func New(server string) *PipeliningClient {
	addr, err := ResolveServerAddr(server)
	if err != nil {
		panic(fmt.Sprintf("Failed to resolve %s", server))
	}

	client := &PipeliningClient{
		addr:       addr,
		serverRepr: server,
	}
	InitWorkerManager(&(client.manager), 10, client)
	return client
}

// Finalize is called in unit tests to free up open connections.
func (c *PipeliningClient) Finalize() {
	c.manager.Finalize()
	c.lk.Lock()
	defer c.lk.Unlock()
	for _, conn := range c.freelist {
		conn.nc.Close()
	}
	c.freelist = nil
}

type ClientInterface interface {
	Get(key string) (item *Item, err error)
	GetMulti(keys []string) (map[string]*Item, error)
	GetMultiArray(keys []string) ([]*Item, error)
	Set(item *Item) error
	Add(item *Item) error
	Replace(item *Item) error
	Increment(key string, delta uint64) (newValue uint64, err error)
	Decrement(key string, delta uint64) (newValue uint64, err error)
	Delete(key string) error
	DeleteAll() error
	Touch(key string, seconds int32) error
}

// PipeliningClient is a memcache client with pipelining.
// It is safe for unlocked use by multiple concurrent goroutines.
type PipeliningClient struct {
	// Timeout specifies the socket read/write timeout.
	// If zero, DefaultTimeout is used.
	Timeout time.Duration

	// MaxIdleConns specifies the maximum number of idle connections that will
	// be maintained per address. If less than one, DefaultMaxIdleConns will be
	// used.
	//
	// Consider your expected traffic rates and latency carefully. This should
	// be set to a number higher than your peak parallel requests.
	MaxIdleConns int

	// The original server address, useful for debugging
	serverRepr string

	addr net.Addr

	// Embedded within the client
	manager WorkerManager

	// This locks access to the free list of connections
	lk       sync.Mutex
	freelist []*conn
}

var _ ClientInterface = &PipeliningClient{}

// Item is an item to be got or stored in a memcached server.
type Item struct {
	// Key is the Item's key (250 bytes maximum).
	Key string

	// Value is the Item's value.
	Value []byte

	// Flags are server-opaque flags whose semantics are entirely
	// up to the app.
	Flags uint32

	// Expiration is the cache expiration time, in seconds: either a relative
	// time from now (up to 1 month), or an absolute Unix epoch time.
	// Zero means the Item has no expiration time.
	Expiration int32

	// Compare and swap ID.
	casid uint64
}

// conn is a connection to a server.
type conn struct {
	nc     net.Conn
	reader *BufferedReader
	// We don't buffer writer - Instead, we write complete commands and flush.
	writer      io.Writer
	addr        net.Addr
	c           *PipeliningClient
	ShouldClose bool
}

func (cn *conn) extendDeadline() {
	cn.nc.SetDeadline(time.Now().Add(cn.c.netTimeout()))
}

func (c *PipeliningClient) putFreeConn(addr net.Addr, cn *conn) {
	c.lk.Lock()
	defer c.lk.Unlock()
	freelist := c.freelist
	if len(freelist) >= c.maxIdleConns() {
		cn.nc.Close()
		return
	}
	c.freelist = append(freelist, cn)
}

func (c *PipeliningClient) getFreeConn(addr net.Addr) (cn *conn, ok bool) {
	c.lk.Lock()
	defer c.lk.Unlock()
	freelist := c.freelist
	if len(freelist) == 0 {
		return nil, false
	}
	cn = freelist[len(freelist)-1]
	c.freelist = freelist[:len(freelist)-1]
	return cn, true
}

func (c *PipeliningClient) netTimeout() time.Duration {
	if c.Timeout != 0 {
		return c.Timeout
	}
	return DefaultTimeout
}

func (c *PipeliningClient) maxIdleConns() int {
	if c.MaxIdleConns > 0 {
		return c.MaxIdleConns
	}
	return DefaultMaxIdleConns
}

// ConnectTimeoutError is the error type used when it takes
// too long to connect to the desired host. This level of
// detail can generally be ignored.
type ConnectTimeoutError struct {
	Addr net.Addr
}

func (cte *ConnectTimeoutError) Error() string {
	return "memcache: connect timeout to " + cte.Addr.String()
}

func (c *PipeliningClient) dial(addr net.Addr) (net.Conn, error) {
	nc, err := net.DialTimeout(addr.Network(), addr.String(), c.netTimeout())
	if err == nil {
		if tcpConn, ok := nc.(*net.TCPConn); ok {
			err = tcpConn.SetWriteBuffer(100000)
			if err != nil {
				fmt.Printf("Failed SetWriteBuffer: %v\n", err)
			}
			err = tcpConn.SetReadBuffer(100000)
			if err != nil {
				fmt.Printf("Failed SetReadBuffer: %v\n", err)
			}
		}
		return nc, nil
	}

	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return nil, &ConnectTimeoutError{addr}
	}

	return nil, err
}

func (c *PipeliningClient) getConn() (*conn, error) {
	addr := c.addr
	cn, ok := c.getFreeConn(addr)
	if ok {
		cn.extendDeadline()
		return cn, nil
	}
	nc, err := c.dial(addr)
	if err != nil {
		return nil, err
	}
	// NoDelay is the default
	cn = &conn{
		nc:   nc,
		addr: addr,
		// XXX testing
		writer: nc, // Not buffered because all users write+flush
		c:      c,
	}
	cn.reader = &BufferedReader{
		reader: bufio.NewReader(nc),
		onClose: func() {
			// XXX this is a race condition
			cn.ShouldClose = true
		},
	}
	return cn, nil
}

func (c *PipeliningClient) FlushAll() error {
	// FIXME uncomment
	//return c.flushAll()
	return nil
}

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (c *PipeliningClient) Get(key string) (item *Item, err error) {
	err = c.get([]string{key}, func(it *Item) { item = it })
	if err == nil && item == nil {
		err = ErrCacheMiss
	}
	return
}

// Touch updates the expiry for the given key. The seconds parameter is either
// a Unix timestamp or, if seconds is less than 1 month, the number of seconds
// into the future at which time the item will expire. ErrCacheMiss is returned if the
// key is not in the cache. The key must be at most 250 bytes in length.
func (c *PipeliningClient) Touch(key string, seconds int32) (err error) {
	return c.touch([]string{key}, seconds)
}

// withWorkerFromPool does the same thing as withConnFromPool, but pipelines requests.
func (c *PipeliningClient) withWorkerFromPool(dataToWrite string, readFn func(*BufferedReader) error) (err error) {
	// Returns error or nil
	return <-c.manager.sendRequestToWorker(dataToWrite, readFn)
}

func (c *PipeliningClient) get(keys []string, cb func(*Item)) error {
	writeCmd := "gets " + strings.Join(keys, " ") + "\r\n"
	//DebugLog("Called get(keys[])")
	return c.withWorkerFromPool(writeCmd, func(r *BufferedReader) error {
		//DebugLog("Calling parseGet")
		return parseGetResponse(r, cb)
	})
}

// flushAll sends the flush_all command to c.addr
func (c *PipeliningClient) flushAll() error {
	return c.withWorkerFromPool("flush_all\r\n", func(r *BufferedReader) error {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultOK):
			break
		default:
			return fmt.Errorf("memcache: unexpected response line from flush_all: %q", string(line))
		}
		return nil
	})
}

func (c *PipeliningClient) touch(keys []string, expiration int32) error {
	buf := bytes.NewBuffer(nil)
	for _, key := range keys {
		if _, err := fmt.Fprintf(buf, "touch %s %d\r\n", key, expiration); err != nil {
			return err
		}
	}

	return c.withWorkerFromPool(buf.String(), func(r *BufferedReader) error {
		// Process results in same order as written
		var err error
		for range keys {
			line, err := r.ReadSlice('\n')
			if err != nil {
				return err
			}
			switch {
			case bytes.Equal(line, resultTouched):
				break
			case bytes.Equal(line, resultNotFound):
				err = ErrCacheMiss
			default:
				// This failed, tell the pool to close the connection
				return fmt.Errorf("memcache: unexpected response line from touch: %q", string(line))
			}
		}
		// Return ErrCacheMiss if any of these missed.
		return err
	})
}

// GetMulti is a batch version of Get. The returned map from keys to
// items may have fewer elements than the input slice, due to memcache
// cache misses. Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
func (c *PipeliningClient) GetMulti(keys []string) (map[string]*Item, error) {
	m := make(map[string]*Item)
	// This is single threaded, has no race conditions.
	err := c.get(keys, func(it *Item) {
		m[it.Key] = it
	})
	return m, err
}

func (c *PipeliningClient) GetMultiArray(keys []string) ([]*Item, error) {
	// TODO: Will this be thread safe when there are multiple servers?
	result := make([]*Item, 0, len(keys))
	err := c.get(keys, func(it *Item) {
		result = append(result, it)
	})
	return result, err
}

// parseGetResponse reads a GET response from r and calls cb for each
// read and allocated Item
func parseGetResponse(r *BufferedReader, cb func(*Item)) error {
	for {
		//DebugLog(fmt.Sprintf("Start readSlice"))
		line, err := r.ReadSlice('\n')
		if err != nil {
			//DebugLog(fmt.Sprintf("Fail readSlice: %v", err))
			return err
		}
		//DebugLog(fmt.Sprintf("Done readSlice: %v", line))
		if bytes.Equal(line, resultEnd) {
			//DebugLog(fmt.Sprintf("Done readSlice, returning: %s", line))
			return nil
		}
		it := new(Item)
		size, err := scanGetResponseLine(line, it)
		if err != nil {
			return err
		}
		it.Value = make([]byte, size+2)
		_, err = io.ReadFull(r, it.Value)
		if err != nil {
			it.Value = nil
			return err
		}
		if !bytes.HasSuffix(it.Value, crlf) {
			it.Value = nil
			return fmt.Errorf("memcache: corrupt get result read")
		}
		it.Value = it.Value[:size]
		cb(it)
	}
}

// scanGetResponseLine populates it and returns the declared size of the item.
// It does not read the bytes of the item.
// This should be equivalent to the commented out code, which was more time consuming due to reflection in Sscanf.
func scanGetResponseLine(line []byte, it *Item) (size int, err error) {
	if len(line) < 13 {
		// "VALUE x 0 1\r\n" is the shortest possible message, and that is 13 bytes long.
		return -1, fmt.Errorf("Line is too short: %q", line)
	}
	if !bytes.Equal(line[:6], valuePrefix) {
		return -1, fmt.Errorf("Expected line to begin with \"VALUE \": %q", line)
	}
	if !bytes.Equal(line[len(line)-2:], crlf) {
		return -1, fmt.Errorf("Expected line to end with \\r\\n: %q", line)
	}
	line = line[6 : len(line)-2]
	parts := bytes.Split(line, space)
	// pattern := "VALUE %s %d %d %d\r\n"
	// dest := []interface{}{&it.Key, &it.Flags, &size, &it.casid}
	partsCount := len(parts)
	if partsCount < 3 || partsCount > 4 {
		return -1, fmt.Errorf("Expected line to match %%s %%s %%d [%%d]: got %q", line)
	}
	// "%s %d %d\n"
	it.Key = string(parts[0])
	if it.Key == "" {
		return -1, fmt.Errorf("memcache: unexpected empty key in %q: %v", line, err)
	}
	flagsRaw, err := strconv.ParseUint(string(parts[1]), 10, 32)
	it.Flags = uint32(flagsRaw)
	if err != nil {
		return -1, fmt.Errorf("memcache: unexpected flags in %q: %v", line, err)
	}
	sizeRaw, err := strconv.ParseInt(string(parts[2]), 10, 0)
	if err != nil {
		return -1, fmt.Errorf("memcache: unexpected size in %q: %v", line, err)
	}
	if partsCount >= 4 {
		// "%s %d %d %d\n"
		it.casid, err = strconv.ParseUint(string(parts[3]), 10, 64)
		if err != nil {
			return -1, fmt.Errorf("memcache: unexpected casid in %q: %v", line, err)
		}
	}
	return int(sizeRaw), nil
}

/*
func scanGetResponseLine(line []byte, it *Item) (size int, err error) {
	if len(line) < 13 {
		return -1, fmt.Errorf("Line is too short: %q", line)
	}
	if !bytes.Equal(line[:6], valuePrefix) {
		return -1, fmt.Errorf("Expected line to begin with \"VALUE \": %q", line)
	}
	if !bytes.Equal(line[len(line)-2:], crlf) {
		return -1, fmt.Errorf("Expected line to end with \\r\\n: %q", line)
	}
	line = line[6 : len(line)-2]
	// Profiling indicates this is expensive
	parts := bytes.Split(line, space)
	// pattern := "VALUE %s %d %d %d\r\n"
	// dest := []interface{}{&it.Key, &it.Flags, &size, &it.casid}
	partsCount := len(parts)
	if partsCount < 3 || partsCount > 4 {
		return -1, fmt.Errorf("Expected line to match %s %s %d [%d]: got %q", line)
	}
	// "%s %d %d\n"
	it.Key = string(parts[0])
	if it.Key == "" {
		return -1, fmt.Errorf("memcache: unexpected empty key in %q: %v", line, err)
	}
	flagsRaw, err := strconv.ParseUint(string(parts[1]), 10, 32)
	it.Flags = uint32(flagsRaw)
	if err != nil {
		return -1, fmt.Errorf("memcache: unexpected flags in %q: %v", line, err)
	}
	sizeRaw, err := strconv.ParseInt(string(parts[2]), 10, 0)
	if err != nil {
		return -1, fmt.Errorf("memcache: unexpected size in %q: %v", line, err)
	}
	if partsCount >= 4 {
		// "%s %d %d %d\n"
		it.casid, err = strconv.ParseUint(string(parts[3]), 10, 64)
		if err != nil {
			return -1, fmt.Errorf("memcache: unexpected casid in %q: %v", line, err)
		}
	}
	return int(sizeRaw), nil
}

/*
func scanGetResponseLine(line []byte, it *Item) (size int, err error) {
	pattern := "VALUE %s %d %d %d\r\n"
	dest := []interface{}{&it.Key, &it.Flags, &size, &it.casid}
	if bytes.Count(line, space) == 3 {
		pattern = "VALUE %s %d %d\r\n"
		dest = dest[:3]
	}
	n, err := fmt.Sscanf(string(line), pattern, dest...)
	if err != nil || n != len(dest) {
		return -1, fmt.Errorf("memcache: unexpected line in get response: %q", line)
	}
	return size, nil
}
*/

// Set writes the given item, unconditionally.
func (c *PipeliningClient) Set(item *Item) error {
	return c.populateOne("set", item)
}

// Add writes the given item, if no value already exists for its
// key. ErrNotStored is returned if that condition is not met.
func (c *PipeliningClient) Add(item *Item) error {
	return c.populateOne("add", item)
}

// Replace writes the given item, but only if the server *does*
// already hold data for this key
func (c *PipeliningClient) Replace(item *Item) error {
	return c.populateOne("replace", item)
}

// CompareAndSwap writes the given item that was previously returned
// by Get, if the value was neither modified or evicted between the
// Get and the CompareAndSwap calls. The item's Key should not change
// between calls but all other item fields may differ. ErrCASConflict
// is returned if the value was modified in between the
// calls. ErrNotStored is returned if the value was evicted in between
// the calls.
func (c *PipeliningClient) CompareAndSwap(item *Item) error {
	return c.populateOne("cas", item)
}

func (c *PipeliningClient) populateOne(verb string, item *Item) error {
	if !legalKey(item.Key) {
		return ErrMalformedKey
	}
	var writeString string
	// FIXME need to support non-utf8 for PHP
	if verb == "cas" {
		writeString = fmt.Sprintf("%s %s %d %d %d %d\r\n%s\r\n",
			verb, item.Key, item.Flags, item.Expiration, len(item.Value), item.casid, item.Value)
	} else {
		writeString = fmt.Sprintf("%s %s %d %d %d\r\n%s\r\n",
			verb, item.Key, item.Flags, item.Expiration, len(item.Value), item.Value)
	}
	return c.withWorkerFromPool(writeString, func(r *BufferedReader) error {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultStored):
			return nil
		case bytes.Equal(line, resultNotStored):
			return ErrNotStored
		case bytes.Equal(line, resultExists):
			return ErrCASConflict
		case bytes.Equal(line, resultNotFound):
			return ErrCacheMiss
		}
		return fmt.Errorf("memcache: unexpected response line from %q: %q", verb, string(line))
	})
}

func readLineAndExpect(reader *BufferedReader, expect []byte) error {
	line, err := reader.ReadSlice('\n')
	if err != nil {
		return err
	}
	switch {
	case bytes.Equal(line, resultOK):
		return nil
	case bytes.Equal(line, expect):
		return nil
	case bytes.Equal(line, resultNotStored):
		return ErrNotStored
	case bytes.Equal(line, resultExists):
		return ErrCASConflict
	case bytes.Equal(line, resultNotFound):
		return ErrCacheMiss
	}
	return fmt.Errorf("memcache: unexpected response line: %q", string(line))
}

// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (c *PipeliningClient) Delete(key string) error {
	return c.withWorkerFromPool("delete "+key+"\r\n", func(r *BufferedReader) error {
		return readLineAndExpect(r, resultDeleted)
	})
}

// DeleteAll deletes all items in the cache.
func (c *PipeliningClient) DeleteAll() error {
	return c.withWorkerFromPool("flush_all\r\n", func(r *BufferedReader) error {
		return readLineAndExpect(r, resultDeleted)
	})
}

// Increment atomically increments key by delta. The return value is
// the new value after being incremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On 64-bit overflow, the new value wraps around.
func (c *PipeliningClient) Increment(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("incr", key, delta)
}

// Decrement atomically decrements key by delta. The return value is
// the new value after being decremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On underflow, the new value is capped at zero and does not wrap
// around.
func (c *PipeliningClient) Decrement(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("decr", key, delta)
}

func (c *PipeliningClient) incrDecr(verb, key string, delta uint64) (uint64, error) {
	var val uint64
	writeString := fmt.Sprintf("%s %s %d\r\n", verb, key, delta)
	err := c.withWorkerFromPool(writeString, func(r *BufferedReader) error {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultNotFound):
			return ErrCacheMiss
		case bytes.HasPrefix(line, resultClientErrorPrefix):
			errMsg := line[len(resultClientErrorPrefix) : len(line)-2]
			return errors.New("memcache: client error: " + string(errMsg))
		}
		val, err = strconv.ParseUint(string(line[:len(line)-2]), 10, 64)
		return err
	})
	return val, err
}

// GetServer returns the address of the server originally passed to memcache.New().
func (c *PipeliningClient) GetServer() string {
	return c.serverRepr
}
