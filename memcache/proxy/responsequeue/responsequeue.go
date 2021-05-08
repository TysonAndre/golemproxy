// responsequeue ensures that responses to requests are sent in the order they are received
package responsequeue

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/TysonAndre/golemproxy/memcache/proxy/message"
)

type ResponseQueue struct {
	m      sync.Mutex
	writer io.Writer
	head   message.Message
	tail   message.Message
	notify chan bool
}

func CreateResponseQueue(writer io.Writer) *ResponseQueue {
	queue := ResponseQueue{}
	queue.writer = writer
	// Make a channel of size 1
	queue.notify = make(chan bool, 1)
	go queue.run()
	return &queue
}

func (queue *ResponseQueue) run() {
	for {
		// TODO: Support closing the channel
		hasEvents := <-queue.notify
		if !hasEvents {
			break
		}
		head := queue.extractEvents()
		err := queue.processEvents(head)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Response writer got unexpected error")
		}
	}
	if closer, ok := queue.writer.(io.Closer); ok {
		fmt.Fprintf(os.Stderr, "Shutting down response writer")
		closer.Close()
	}
}

func (queue *ResponseQueue) Close() {
	close(queue.notify)
}

func getLinkedListNext(m message.Message) *message.Message {
	// Can probably optimize with unsafe?
	if single, ok := m.(*message.SingleMessage); ok {
		return &single.NextOutgoingResponse
	}
	multi := m.(*message.FragmentedMessage)
	return &multi.NextOutgoingResponse
}

func (queue *ResponseQueue) extractEvents() message.Message {
	queue.m.Lock()
	defer queue.m.Unlock()
	if queue.head == nil {
		return nil
	}
	// Detach the linked list from the queue
	head := queue.head
	queue.head = nil
	queue.tail = nil
	return head
}

// After detaching the linked list from the queue, process all events in that linked list
func (queue *ResponseQueue) processEvents(response message.Message) error {
	for response != nil {
		// TODO: Non-blocking check if the response was sent, so that messages can be combined for clients that pipeline?
		data, err := response.AwaitResponseBytes()
		if err != nil {
			data = err.ErrorBytes
		}
		if len(data) == 0 {
			panic("Expected response data")
		}
		_, writeErr := queue.writer.Write(data)
		if writeErr != nil {
			return writeErr
		}
		response = *getLinkedListNext(response)
	}
	return nil
}

// RecordOutgoingRequest tracks an outgoing request so that responses to pipelined requests caan be sent in order.
// It is called only by the goroutine that accepts messages from a client of the proxy
func (queue *ResponseQueue) RecordOutgoingRequest(message message.Message) {
	// Precondition: A reply from the server is expected for the given message

	// A channel is not used to avoid blocking the goroutine that handles communication with remote servers, if writing to the requestor blocks.
	// A slow client of the proxy should not block fast clients of the proxy
	queue.m.Lock()
	if queue.tail != nil {
		*getLinkedListNext(queue.tail) = message
		queue.tail = message
	} else {
		queue.tail = message
		queue.head = message
	}
	queue.m.Unlock()

	// Asynchronously notify that there are responses to be sent to the client by either adding to the queue or or doing nothing
	select {
	case queue.notify <- true:
	default:
	}
}
