// proxy listens on a socket and forwards data to one or more memcache servers (TODO: Actually shard requests)
package proxy

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/TysonAndre/golemproxy/byteutil"
	"github.com/TysonAndre/golemproxy/config"
	"github.com/TysonAndre/golemproxy/memcache"
	"github.com/TysonAndre/golemproxy/memcache/proxy/message"
	"github.com/TysonAndre/golemproxy/memcache/proxy/responsequeue"
	"github.com/TysonAndre/golemproxy/sharded"
	"go4.org/strutil"
)

var (
	// these are all used as constants
	noreplyBytes = []byte("noreply")

	requestAdd     = []byte("add")
	requestAppend  = []byte("append")
	requestDelete  = []byte("delete")
	requestIncr    = []byte("incr")
	requestDecr    = []byte("decr")
	requestGet     = []byte("get")
	requestGets    = []byte("gets")
	requestPrepend = []byte("prepend")
	requestReplace = []byte("replace")
	requestSet     = []byte("set")
)

const MAX_ITEM_SIZE = 1 << 20

// itob converts an integer to the bytes to represent that integer
func itob(value int) []byte {
	// TODO: optimize
	return []byte(strconv.Itoa(value))
}

func indexByteOffset(data []byte, c byte, offset int) int {
	for n := len(data); offset < n; offset++ {
		if data[offset] == c {
			return offset
		}
	}
	return -1
}

// splitArgsOnSpaces returns 0 or more non-empty byte slices that do not contain spaces.
func splitArgsOnSpaces(data []byte) ([][]byte, error) {
	parts := [][]byte{}
	start := 0
	for i, c := range data {
		if c == ' ' {
			if i == start {
				// Some other proxies or protocol implementations may have issues with extra spaces
				return nil, errors.New("Unexpected extra space in memcache command")
			}
			parts = append(parts, data[start:i])
			start = i + 1
		}
	}
	if start >= len(data) {
		// python-memcached is sending "set kkk-0 16 0 5 \r\n"
		/*
			if start > 0 {
				return nil, errors.New("Unexpected extra space in memcache command")
			}
		*/
		return parts, nil
	}
	return append(parts, data[start:]), nil
}

// handleGet forwards the 'get' or 'gets' (with CAS) request to a memcache client and sends a response back
// request is "get key1 key2 key3\r\n"
func handleGet(requestHeader []byte, responses *responsequeue.ResponseQueue, remote memcache.ClientInterface) error {
	// TODO: Check for malformed get command (e.g. stray \r)

	keyI := bytes.IndexByte(requestHeader, ' ')
	if keyI < 0 {
		return errors.New("missing space")
	}
	keys, err := splitArgsOnSpaces(requestHeader[keyI+1 : len(requestHeader)-2])
	if err != nil {
		return err
	}

	if len(keys) == 0 {
		return errors.New("missing key")
	}
	if len(keys) == 1 {
		m := &message.SingleMessage{}
		key := keys[0]
		// fmt.Fprintf(os.Stderr, "handleGet %q key=%v\n", string(requestHeader), string(key))
		m.HandleSendRequest(requestHeader, key, message.REQUEST_MC_GET)
		remote.SendProxiedMessageAsync(m)
		responses.RecordOutgoingRequest(m)
		return nil
	}
	fragments := make([]message.SingleMessage, len(keys))
	for i, key := range keys {
		requestFragment := make([]byte, keyI+3+len(key))
		// 'get ' + key + '\r\n'
		copy(requestFragment, requestHeader[:keyI+1])
		copy(requestFragment[keyI+1:], key)
		copy(requestFragment[keyI+1+len(key):], "\r\n")
		m := &fragments[i]
		m.HandleSendRequest(requestFragment, key, message.REQUEST_MC_GET)
		remote.SendProxiedMessageAsync(m)
		// responses.RecordOutgoingRequest(m)
	}

	fragmentedRequest := &message.FragmentedMessage{
		Fragments: fragments,
	}
	responses.RecordOutgoingRequest(fragmentedRequest)

	return nil
}

func handleDelete(requestHeader []byte, responses *responsequeue.ResponseQueue, remote memcache.ClientInterface) error {
	// TODO: Check for malformed delete command (e.g. stray \r)
	m := &message.SingleMessage{}

	keyI := bytes.IndexByte(requestHeader, ' ')
	if keyI < 0 {
		return errors.New("missing space")
	}
	args, err := splitArgsOnSpaces(requestHeader[keyI+1 : len(requestHeader)-2])
	if err != nil {
		return err
	}
	if len(args) < 1 || len(args) > 2 {
		return errors.New("unexpected arg count for " + string(requestHeader[:4]))
	}
	noreply := false
	if len(args) == 2 {
		if !bytes.Equal(args[1], noreplyBytes) {
			return errors.New("expected extra arg to be noreply")
		}
		noreply = true
	}

	key := args[0]
	m.HandleSendRequest(requestHeader, key, message.REQUEST_MC_DELETE)
	remote.SendProxiedMessageAsync(m)
	if !noreply {
		responses.RecordOutgoingRequest(m)
	}
	return nil
}

func handleIncrOrDecr(requestHeader []byte, responses *responsequeue.ResponseQueue, remote memcache.ClientInterface) error {
	// TODO: Check for malformed delete command (e.g. stray \r)
	m := &message.SingleMessage{}

	keyI := bytes.IndexByte(requestHeader, ' ')
	if keyI < 0 {
		return errors.New("missing space")
	}
	args, err := splitArgsOnSpaces(requestHeader[keyI+1 : len(requestHeader)-2])
	if err != nil {
		return err
	}
	if len(args) < 2 || len(args) > 3 {
		return fmt.Errorf("unexpected arg count %d for %s", len(args), string(requestHeader[:4]))
	}
	if !byteutil.IsExclusivelyDigits(args[1]) {
		return errors.New("expected incr or decr to be a number")
	}
	noreply := false
	if len(args) == 3 {
		if !bytes.Equal(args[2], noreplyBytes) {
			return errors.New("expected extra arg to be noreply")
		}
		noreply = true
	}

	key := args[0]
	m.HandleSendRequest(requestHeader, key, message.REQUEST_MC_INCR)
	remote.SendProxiedMessageAsync(m)
	// If a request includes 'noreply' then the server would not send back a response.
	if !noreply {
		responses.RecordOutgoingRequest(m)
	}
	return nil
}

// handleSet forwards a set request to the memcache servers and returns a result.
// TODO: Add the capability to mock successful responses before sending the request
func handleSet(requestHeader []byte, reader *bufio.Reader, responses *responsequeue.ResponseQueue, remote memcache.ClientInterface) error {
	// FIXME support 'noreply'
	// parse the number of bytes then read
	// requestHeader is set|add|replace|insert key <flags> <expiry> <valuelen> [noreply]\r\n<value>\r\n
	args, err := splitArgsOnSpaces(requestHeader[:len(requestHeader)-2])
	if err != nil {
		return fmt.Errorf("could not parse %q: %v\n", string(requestHeader), err)
	}
	if len(args) < 5 || len(args) > 6 {
		cmd := string(args[0])
		return fmt.Errorf("unexpected word count %d for %s, expected '%s key flags expiry valuelen [noreply]'", len(args), cmd, cmd)
	}

	// TODO: use https://godoc.org/go4.org/strutil#ParseUintBytes
	_, err = strutil.ParseUintBytes(args[2], 10, 32)
	if err != nil {
		return fmt.Errorf("failed to parse flags: %v", err)
	}
	_, err = strutil.ParseUintBytes(args[3], 10, 32)
	if err != nil {
		return fmt.Errorf("failed to parse expiry: %v", err)
	}
	length, err := strutil.ParseUintBytes(args[4], 10, 30)
	if err != nil {
		return fmt.Errorf("failed to parse length: %v", err)
	}
	if length < 0 {
		return fmt.Errorf("Wrong length: expected non-negative value")
	}
	if length > MAX_ITEM_SIZE {
		return fmt.Errorf("Wrong length: %d exceeds MAX_ITEM_SIZE of %d", length, MAX_ITEM_SIZE)
	}
	noreply := false
	if len(args) == 6 {
		if !bytes.Equal(args[5], noreplyBytes) {
			return errors.New("expected extra arg to be noreply")
		}
		noreply = true
	}
	fullRequestLength := len(requestHeader) + int(length) + 2
	requestBody := make([]byte, fullRequestLength)
	copy(requestBody, requestHeader)
	n, err := io.ReadFull(reader, requestBody[len(requestHeader):])
	if err != nil {
		return fmt.Errorf("Failed to read %d requestBody, got %d: %v", length, n, err)
	}
	// skip \r\n
	if requestBody[fullRequestLength-2] != '\r' || requestBody[fullRequestLength-1] != '\n' {
		return fmt.Errorf("Value was not followed by \\r\\n")
	}
	m := &message.SingleMessage{}

	key := args[1]
	m.HandleSendRequest(requestBody, key, message.REQUEST_MC_SET)
	remote.SendProxiedMessageAsync(m)
	if !noreply {
		responses.RecordOutgoingRequest(m)
	}
	return nil
}

func handleCommand(reader *bufio.Reader, responses *responsequeue.ResponseQueue, remote memcache.ClientInterface) error {
	// ReadBytes is safe to reuse, ReadSlice isn't.
	header, err := reader.ReadBytes('\n')
	if err != nil {
		// Check if the reader exited cleanly
		if err != io.EOF {
			// TODO: Handle EOF
			fmt.Fprintf(os.Stderr, "ReadSlice failed: %s\n", err.Error())
		}
		return err
	}
	headerLen := len(header)
	if headerLen < 2 {
		return errors.New("request too short")
	}

	i := bytes.IndexByte(header, ' ')
	if i <= 1 {
		return errors.New("empty request")
	}
	// If a client sends the carriage return in the wrong place, close that client,
	// the client might not be properly validating keys.
	carriageReturnPos := bytes.IndexByte(header, '\r')
	if carriageReturnPos != headerLen-2 {
		if carriageReturnPos < 0 {
			return errors.New("request header did not have carriage return in the expected position")
		} else {
			return errors.New("request header had carriage return in an unexpected position")
		}
	}

	// fmt.Fprintf(os.Stderr, "got request %q i=%d\n", header, i)
	switch i {
	case 3:
		// memcached protocol is case sensitive
		if bytes.HasPrefix(header, requestGet) {
			err := handleGet(header, responses, remote)
			if err != nil {
				fmt.Fprintf(os.Stderr, "get request parsing failed: %s\n", err.Error())
			}
			return err
		}
		if bytes.HasPrefix(header, requestSet) || bytes.HasPrefix(header, requestAdd) {
			err := handleSet(header, reader, responses, remote)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s request parsing failed: %s\n", string(header[:3]), err.Error())
			}
			return err
		}
	case 4:
		// memcached protocol is case sensitive
		if bytes.HasPrefix(header, requestGets) {
			err := handleGet(header, responses, remote)
			if err != nil {
				fmt.Fprintf(os.Stderr, "gets request parsing failed: %s\n", err.Error())
			}
			return err
		}
		if bytes.HasPrefix(header, requestIncr) {
			err := handleIncrOrDecr(header, responses, remote)
			if err != nil {
				fmt.Fprintf(os.Stderr, "incr request parsing failed: %s\n", err.Error())
			}
			return err
		}
		if bytes.HasPrefix(header, requestDecr) {
			err := handleIncrOrDecr(header, responses, remote)
			if err != nil {
				fmt.Fprintf(os.Stderr, "decr request parsing failed: %s\n", err.Error())
			}
			return err
		}
	case 6:
		if bytes.HasPrefix(header, requestDelete) {
			err := handleDelete(header, responses, remote)
			if err != nil {
				fmt.Fprintf(os.Stderr, "delete request parsing failed: %s\n", err.Error())
			}
			return err
		}
		if bytes.HasPrefix(header, requestAppend) {
			err := handleSet(header, reader, responses, remote)
			if err != nil {
				fmt.Fprintf(os.Stderr, "append request parsing failed: %s\n", err.Error())
			}
			return err
		}
	case 7:
		if bytes.HasPrefix(header, requestDelete) {
			err := handleDelete(header, responses, remote)
			if err != nil {
				fmt.Fprintf(os.Stderr, "delete request parsing failed: %s\n", err.Error())
			}
			return err
		}
		// replace and prepend have the same arg count as set
		if bytes.HasPrefix(header, requestReplace) || bytes.HasPrefix(header, requestPrepend) {
			err := handleSet(header, reader, responses, remote)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s request parsing failed: %s\n", string(header[:7]), err.Error())
			}
			return err
		}
	}
	fmt.Fprintf(os.Stderr, "Unknown command %q\n", header)
	return errors.New("unknown command")
}

// serveSocket runs in a loop to read memcached requests and send memcached responses
func serveSocket(remote memcache.ClientInterface, c net.Conn) {
	reader := bufio.NewReader(c)
	responseQueue := responsequeue.CreateResponseQueue(c)

	for {
		err := handleCommand(reader, responseQueue, remote)
		if err != nil {
			c.Close()
			return
		}
	}
}

func handleUnexpectedExit(listeners []net.Listener, didExit *bool) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt, os.Kill, syscall.SIGTERM)
	go func(c chan os.Signal) {
		// Wait for a SIGINT or SIGKILL:
		sig := <-c
		*didExit = true
		fmt.Fprintf(os.Stderr, "Caught signal %s: shutting down.\n", sig)
		for _, l := range listeners {
			// Stop listening (and unlink the socket if unix type):
			l.Close()
		}
		// And we're done:
		os.Exit(0)
	}(sigc)
}

func createUnixSocket(path string, serverType string) (net.Listener, error) {
	fmt.Fprintf(os.Stderr, "Listening for %s requests at unix socket %q\n", serverType, path)
	l, err := net.Listen("unix", path)
	return l, err
}

func createTCPSocket(path string, serverType string) (net.Listener, error) {
	fmt.Fprintf(os.Stderr, "Listening for %s requests at tcp server %q\n", serverType, path)
	l, err := net.Listen("tcp", path)
	return l, err
}

func serveSocketServer(remote memcache.ClientInterface, l net.Listener, path string, didExit *bool) {
	for {
		fd, err := l.Accept()
		if *didExit {
			return
		}
		if err != nil {
			// TODO: Clean up debug code
			fmt.Fprintf(os.Stderr, "accept error for %q: %v", path, err)
			return
		}

		go serveSocket(remote, fd)
	}
}

func serveStatsServer(statsPortFlag uint, didExit *bool) {
	if statsPortFlag == 0 || statsPortFlag >= (1<<16) {
		return
	}
	var l net.Listener
	var err error

	statsServerAddr := fmt.Sprintf("127.0.0.1:%d", statsPortFlag)
	l, err = createTCPSocket(statsServerAddr, "stats")
	if err != nil {
		// TODO: Clean up the rest of the sockets
		fmt.Fprintf(os.Stderr, "Listen error at %s: %v\n", statsServerAddr, err)
		return
	}

	go func() {
		for {
			fd, err := l.Accept()
			if *didExit {
				return
			}
			if err != nil {
				// TODO: Clean up debug code
				fmt.Fprintf(os.Stderr, "accept error for %s: %v", statsServerAddr, err)
				return
			}

			go func() {
				data := map[string]interface{}{
					"command": "golemproxy",
				}
				bytes, err := json.Marshal(data)
				if err != nil {
					bytes = append([]byte("ERROR: "), []byte(err.Error())...)
				}
				bytes = append(bytes, '\r', '\n')
				fd.Write(bytes)

				fd.Close()
			}()
		}
	}()
}

func Run(configs map[string]config.Config, statsPort uint) {
	var wg sync.WaitGroup
	wg.Add(len(configs))

	didExit := false
	listeners := []net.Listener{}

	for _, config := range configs {
		remote := sharded.New(config)
		socketPath := config.Listen
		// TODO: Also support tcp sockets
		var l net.Listener
		var err error
		i := strings.IndexRune(socketPath, ':')
		if i >= 0 {
			l, err = createTCPSocket(socketPath, "memcache")
		} else {
			l, err = createUnixSocket(socketPath, "memcache")
		}
		if err != nil {
			// TODO: Clean up the rest of the sockets
			fmt.Fprintf(os.Stderr, "Listen error at %s: %v\n", socketPath, err)
			for _, l := range listeners {
				l.Close()
			}
			return
		}
		listeners = append(listeners, l)

		go func() {
			defer l.Close()
			serveSocketServer(remote, l, socketPath, &didExit)
			wg.Done()
		}()
	}
	serveStatsServer(statsPort, &didExit)

	handleUnexpectedExit(listeners, &didExit)
	wg.Wait()
}
