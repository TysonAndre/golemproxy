// This package listens on a socket and forwards data to one or more memcache servers
package server

import (
	"bufio"
	"bytes"
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

	"github.com/TysonAndre/golemproxy/config"
	"github.com/TysonAndre/golemproxy/memcache"
	"github.com/TysonAndre/golemproxy/memcache/sharded"
	"go4.org/strutil"
)

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

// extractKeys extracts space-separated memcached keys from the bytes of a line
func extractKeys(line []byte) []string {
	parts := bytes.Split(line, space)
	result := make([]string, 0, len(line))
	for _, val := range parts {
		if len(val) == 0 {
			continue
		}
		result = append(result, string(val))
	}
	return result
}

// itob converts an integer to the bytes to represent that integer
func itob(value int) []byte {
	// TODO: optimize
	return []byte(strconv.Itoa(value))
}

// makeResponsePayloadFromItems generates a response payload for the memcache client
func makeResponsePayloadFromItems(items []*memcache.Item) []byte {
	// TODO: Look into reusing byte slices, like twemproxy.
	if len(items) == 0 {
		return resultEnd
	}
	payload := []byte{}
	for _, item := range items {
		// TODO: Support CAS
		// "VALUE" key flags length [casid]\r\nvalue\r\n
		payload = append(payload, valuePrefix...)
		payload = append(payload, []byte(item.Key)...)
		payload = append(payload, ' ')
		payload = append(payload, itob(int(item.Flags))...)
		payload = append(payload, ' ')
		payload = append(payload, itob(len(item.Value))...)
		payload = append(payload, crlf...)
		payload = append(payload, item.Value...)
		payload = append(payload, crlf...)
	}
	payload = append(payload, resultEnd...)
	return payload
}

// handleGet forwards the GET request to a memcache client and sends a response back
func handleGet(reader *bufio.Reader, writer io.Writer, remote memcache.ClientInterface) error {
	rawkeys, err := reader.ReadSlice('\n')
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read rest: rawkeys=%s err=%v", rawkeys, err)
		return err
	}
	if len(rawkeys) < 3 {
		fmt.Fprintf(os.Stderr, "Failed to read any keys: len=%d", len(rawkeys))
		return errors.New("failed to read keys")
	}
	keys := extractKeys(rawkeys[:len(rawkeys)-2])
	items, err := remote.GetMultiArray(keys)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fetching failed: %v\n", err)
	}
	payload := makeResponsePayloadFromItems(items)
	fmt.Fprintf(os.Stderr, "Sending this response for %v:\n%s", keys, payload)
	_, err = writer.Write(payload)
	if err != nil {
		return fmt.Errorf("Failed to send response: %v", err)
	}
	return nil
}

// handleSet forwards a set request to the memcache servers and returns a result.
// TODO: Add the capability to mock successful responses before sending the request
func handleSet(reader *bufio.Reader, writer io.Writer, remote memcache.ClientInterface) error {
	// parse the number of bytes then read
	line, err := reader.ReadSlice('\n')
	if err != nil {
		return err
	}
	parts := bytes.Split(line[:len(line)-2], space)
	if len(parts) != 4 {
		return fmt.Errorf("unexpected word count %d for set", len(parts))
	}
	key := string(parts[0])

	// TODO: use https://godoc.org/go4.org/strutil#ParseUintBytes
	flags, err := strutil.ParseUintBytes(parts[1], 10, 32)
	if err != nil {
		return fmt.Errorf("failed to parse flags: %v", err)
	}
	expiry, err := strutil.ParseUintBytes(parts[2], 10, 32)
	if err != nil {
		return fmt.Errorf("failed to parse expiry: %v", err)
	}
	length, err := strutil.ParseUintBytes(parts[3], 10, 30)
	if err != nil {
		return fmt.Errorf("failed to parse length: %v", err)
	}
	if length < 0 {
		return fmt.Errorf("Wrong length: expected non-negative value")
	}
	bytes := make([]byte, length)
	n, err := io.ReadFull(reader, bytes)
	if err != nil {
		return fmt.Errorf("Failed to read %d bytes, got %d: %v", length, n, err)
	}
	// skip \r\n
	_, err = reader.Discard(2)
	if err != nil {
		return fmt.Errorf("Failed to read \\r\\n after set: %v", err)
	}

	item := &memcache.Item{
		Key:        key,
		Flags:      uint32(flags),
		Expiration: int32(expiry),
		Value:      bytes,
	}
	fmt.Fprintf(os.Stderr, "Read an item: %#v\n", item)
	// TODO: Turn this back into a byte response or preserve bytes
	memcachedErr := remote.Set(item)
	if memcachedErr == nil {
		_, err = writer.Write(resultStored)
	} else {
		_, err = writer.Write(resultNotStored)
	}
	if err != nil {
		return fmt.Errorf("Failed to write memcached set response: %v", err)
	}
	return nil
}

func handleCommand(reader *bufio.Reader, writer io.Writer, remote memcache.ClientInterface) error {
	cmd, err := reader.ReadSlice(' ')
	if err != nil {
		// Check if the reader exited cleanly
		if err != io.EOF {
			// TODO: Handle EOF
			fmt.Fprintf(os.Stderr, "ReadSlice failed: %s\n", err.Error())
		}
		return err
	}
	fmt.Printf("Read command: %q\n", cmd)

	switch strings.ToLower(string(cmd[:len(cmd)-1])) {
	case "get":
		err := handleGet(reader, writer, remote)
		if err != nil {
			fmt.Fprintf(os.Stderr, "GET failed: %s\n", err.Error())
		}
		return err
	case "set":
		err := handleSet(reader, writer, remote)
		if err != nil {
			fmt.Fprintf(os.Stderr, "SET failed: %s\n", err.Error())
		}
		return err
	default:
		return errors.New("unknown command")
	}
}

// serveSocket runs in a loop to read memcached requests and send memcached responses
func serveSocket(remote memcache.ClientInterface, c net.Conn) {
	reader := bufio.NewReader(c)

	for {
		err := handleCommand(reader, c, remote)
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

func createUnixSocket(path string) (net.Listener, error) {
	fmt.Fprintf(os.Stderr, "Listening for memcache requests at %q\n", path)
	l, err := net.Listen("unix", path)
	return l, err
}

func serveSocketServer(remote memcache.ClientInterface, l net.Listener, path string, didExit *bool) {
	for {
		fd, err := l.Accept()
		if *didExit {
			return
		}
		if err != nil {
			println("accept error", err.Error())
			fmt.Fprintf(os.Stderr, "accept error for %q: %v", path, err)
			return
		}

		go serveSocket(remote, fd)
	}
}

func Run(configs map[string]config.Config) {
	var wg sync.WaitGroup
	wg.Add(len(configs))

	didExit := false
	listeners := []net.Listener{}

	for _, config := range configs {
		remote := sharded.New(config)
		socketPath := config.Listen
		l, err := createUnixSocket(socketPath)
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
	handleUnexpectedExit(listeners, &didExit)
	wg.Wait()
}
