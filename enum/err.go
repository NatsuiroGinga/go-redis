package enum

import "errors"

var (
	CONNECTION_CLOSED = errors.New("use of closed network connection")
	EMPTY_PAYLOAD     = errors.New("empty payload")
	SERVER_TIMEOUT    = errors.New("server time out")
	REQUEST_FAILED    = errors.New("request failed")
	NOT_SUPPORTED_CMD = errors.New("not supported command")
)
