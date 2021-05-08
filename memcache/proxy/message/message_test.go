package message

import (
	"testing"

	"github.com/TysonAndre/golemproxy/testutil"
)

var NIL_SERVER_ERROR *ResponseError = nil

func TestAwaitResponseBytes(t *testing.T) {
	expectedRequest := "get key\r\n"
	m := SingleMessage{}
	m.HandleSendRequest([]byte(expectedRequest), []byte("key"), REQUEST_MC_GET)

	testutil.ExpectEquals(t, []byte(expectedRequest), m.RequestData, "unexpected data")
	testutil.ExpectEquals(t, REQUEST_MC_GET, m.RequestType, "unexpected request type")

	// VALUE <key> <0> <7>
	expectedResponse := "VALUE key 0 7\r\nmyvalue\r\nEND\r\n"
	m.HandleReceiveResponse([]byte(expectedResponse), RESPONSE_MC_VALUE)

	actualData, actualErr := m.AwaitResponseBytes()
	testutil.ExpectEquals(t, []byte(expectedResponse), actualData, "unexpected data")
	testutil.ExpectEquals(t, NIL_SERVER_ERROR, actualErr, "expected nil error")
	testutil.ExpectEquals(t, RESPONSE_MC_VALUE, m.ResponseType, "unexpected response type")
}
