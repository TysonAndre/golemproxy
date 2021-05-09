// message contains utilities for proxying the sending out of a memcached request and receiving a memcached response
package message

import (
	"sync"
)

const (
	RESPONSE_MC_PROTOCOLERROR ResponseType = 0
	RESPONSE_MC_VALUE         ResponseType = 1
	RESPONSE_MC_END           ResponseType = 2
	RESPONSE_MC_STORED        ResponseType = 3
	RESPONSE_MC_NOT_STORED    ResponseType = 4
	RESPONSE_MC_EXISTS        ResponseType = 5
	RESPONSE_MC_NOT_FOUND     ResponseType = 6
	RESPONSE_MC_DELETED       ResponseType = 7
	RESPONSE_MC_TOUCHED       ResponseType = 8
	RESPONSE_MC_OK            ResponseType = 8
	RESPONSE_MC_NUMBER        ResponseType = 9
)

const (
	REQUEST_MC_UNKNOWN RequestType = 1
	REQUEST_MC_GET     RequestType = 2
	REQUEST_MC_GETS    RequestType = 3
	REQUEST_MC_SET     RequestType = 4
	REQUEST_MC_DELETE  RequestType = 5
	REQUEST_MC_INCR    RequestType = 6
	REQUEST_MC_CAS     RequestType = 7
)

type RequestType uint8
type ResponseType uint8

// Message is the representation of a well-formed request for 1 or more keys
type Message interface {
	AwaitResponseBytes() ([]byte, *ResponseError)
}

var _ Message = &SingleMessage{}
var _ Message = &FragmentedMessage{}

const END_LINE_LENGTH = 5 // END\r\n

type MessageLinkedListEntry struct {
	NextOutgoingResponse Message
}

type SingleMessage struct {
	MessageLinkedListEntry
	// Mutex is locked by the creator of the message and released after succeeding or failing at receiving a result.
	Mutex         sync.Mutex
	RequestData   []byte
	ResponseData  []byte
	ResponseError *ResponseError
	Key           []byte
	ResponseType  ResponseType
	RequestType   RequestType
}

// A message that affects multiple keys, possibly on different backends. Currently just memcache multigets.
type FragmentedMessage struct {
	MessageLinkedListEntry
	// Pointers to 2 or more non-null messages
	Fragments []SingleMessage
}

// type MessageCombiner func([]*SingleMessage) ([]byte, *ResponseError)

// 1. A message is sent and received by the proxy, locking the mutex
// 2. A message is proxied to a server and receives a response or an error, unlocking the mutex
// 3. A connection on the proxy waits to lock the mutext to receive the response/error.

func (message *SingleMessage) HandleSendRequest(data []byte, key []byte, requestType RequestType) {
	// fmt.Fprintf(os.Stderr, "HandleSendRequest %q %q\n", string(data), string(key))
	message.Mutex.Lock()
	message.RequestData = data
	message.Key = key
	message.RequestType = requestType
}

func (message *SingleMessage) HandleReceiveResponse(data []byte, responseType ResponseType) {
	message.ResponseData = data
	message.ResponseType = responseType
	message.Mutex.Unlock()
}

func (message *SingleMessage) HandleReceiveError(err error) {
	// fmt.Fprintf(os.Stderr, "TODO: Handle error %v\n", err)
	message.ResponseError = RESPONSE_ERROR_UNEXPECTED_TYPE
	message.Mutex.Unlock()
}

func (message *SingleMessage) AwaitResponseBytes() ([]byte, *ResponseError) {
	message.Mutex.Lock()
	return message.ResponseData, message.ResponseError
}

func (message *FragmentedMessage) AwaitResponseBytes() ([]byte, *ResponseError) {
	// This will await all responses separately and combine them.
	return CombineMemcacheMultiget(message.Fragments)
}

// Combine the VALUE key\r\n
func CombineMemcacheMultiget(fragments []SingleMessage) ([]byte, *ResponseError) {
	var combination []byte
	n := len(fragments)
	for i := 0; i < n; i++ {
		messageFragment := &fragments[i]
		responseOfFragment, err := messageFragment.AwaitResponseBytes()
		if err != nil {
			// No need to wait for the other responses
			return nil, err
		}
		if messageFragment.ResponseType != RESPONSE_MC_VALUE && messageFragment.ResponseType != RESPONSE_MC_END {
			return nil, RESPONSE_ERROR_UNEXPECTED_TYPE
		}
		// expect part to end in "END\r\n" - add this to the last buffer and remove it from other buggers
		if i == n-1 {
			combination = append(combination, responseOfFragment...)
			break
		} else {
			combination = append(combination, responseOfFragment[:len(responseOfFragment)-END_LINE_LENGTH]...)
		}
	}
	// fmt.Fprintf(os.Stderr, "Combined response=%q\n", string(combination))
	return combination, nil
}
