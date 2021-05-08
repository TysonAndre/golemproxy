package responsequeue

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/TysonAndre/golemproxy/memcache/proxy/message"
	"github.com/TysonAndre/golemproxy/testutil"
)

func TestResponseQueueEmpty(t *testing.T) {
	mockWriter := &bytes.Buffer{}
	queue := CreateResponseQueue(mockWriter)
	queue.Close()
	testutil.ExpectEquals(t, []byte(nil), mockWriter.Bytes(), "should close successfully")
}

type MockWriter struct {
	queue chan []byte
}

func NewMockWriter() *MockWriter {
	return &MockWriter{
		queue: make(chan []byte),
	}
}

func (w *MockWriter) Write(msg []byte) (int, error) {
	w.queue <- msg
	return len(msg), nil
}

func TestResponseQueueHandleMessage(t *testing.T) {
	mockWriter := NewMockWriter()
	queue := CreateResponseQueue(mockWriter)
	go func() {
		m := message.SingleMessage{}
		m.HandleSendRequest([]byte("get key\r\n"), []byte("key"), message.REQUEST_MC_GET)
		queue.RecordOutgoingRequest(&m)
		m.HandleReceiveResponse([]byte("END\r\n"), message.RESPONSE_MC_END)
	}()
	payload := <-mockWriter.queue
	testutil.ExpectEquals(t, []byte("END\r\n"), payload, "should receive response")
}

func TestResponseQueueHandleMessageCombine(t *testing.T) {
	iterations := 4
	mockWriter := NewMockWriter()
	queue := CreateResponseQueue(mockWriter)
	// Responses should be added to the queue in the order that RecordOutgoingRequest is called
	go func() {
		for i := 0; i < iterations; i++ {
			m1 := message.SingleMessage{}
			m1.HandleSendRequest([]byte("get key\r\n"), []byte("key"), message.REQUEST_MC_GET)
			queue.RecordOutgoingRequest(&m1)
			m2 := message.SingleMessage{}
			m2.HandleSendRequest([]byte("get key2\r\n"), []byte("key2"), message.REQUEST_MC_GET)
			queue.RecordOutgoingRequest(&m2)

			// The responses can be received out of order when memcached is sharded to multiple memcached servers.
			// Emulate that.
			m2.HandleReceiveResponse([]byte("END\r\n"), message.RESPONSE_MC_END)
			m1.HandleReceiveResponse([]byte("VALUE key 0 4\r\nabcd\r\nEND\r\n"), message.RESPONSE_MC_END)
		}
	}()
	expectedPart := "VALUE key 0 4\r\nabcd\r\nEND\r\nEND\r\n"
	expected := ""
	for i := 0; i < iterations; i++ {
		expected += expectedPart
	}

	actual := ""
	for len(actual) < len(expected) {
		payload := <-mockWriter.queue
		fmt.Fprintf(os.Stderr, "response=%s\n", string(payload))
		actual += string(payload)
	}
	testutil.ExpectEquals(t, expected, actual, "should receive response")
}
