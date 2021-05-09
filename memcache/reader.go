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

// ReadBytes returns a brand new byte slice that does not overlap with other byte slices
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
