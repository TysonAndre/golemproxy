package message

type ResponseError struct {
	ErrorBytes []byte
}

var _ error = &ResponseError{}

func NewResponseError(message []byte) *ResponseError {
	return &ResponseError{
		ErrorBytes: message,
	}
}

func (error *ResponseError) Error() string {
	return string(error.ErrorBytes)
}

var RESPONSE_ERROR_UNEXPECTED_TYPE = NewResponseError([]byte("SERVER_ERROR multiget fail\r\n"))
var RESPONSE_ERROR_UNKNOWN_COMMAND = NewResponseError([]byte("SERVER_ERROR unknown command\r\n"))
