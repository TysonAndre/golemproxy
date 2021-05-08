package memcache

import (
	"bufio"
	"errors"
)

type BufferedReader struct {
	reader  *bufio.Reader
	failed  bool
	onClose func()
}

var ErrPreviousRequestFailed = errors.New("A previous request failed")

// ReadBytes returns a copy of bytes that lasts until the next read from the buffer
func (reader *BufferedReader) ReadBytes(delim byte) ([]byte, error) {
	if reader.failed == true {
		return nil, ErrPreviousRequestFailed
	}
	result, err := reader.reader.ReadBytes(delim)
	if err != nil {
		reader.handleError()
	}
	return result, err
}

func (reader *BufferedReader) handleError() {
	reader.failed = true
	reader.onClose()
}

func (reader *BufferedReader) Read(p []byte) (int, error) {
	if reader.failed == true {
		return 0, ErrPreviousRequestFailed
	}
	n, err := reader.reader.Read(p)
	if err != nil {
		reader.handleError()
	}
	return n, err
}
