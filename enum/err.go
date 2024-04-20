package enum

import "errors"

var (
	CONNECTION_CLOSED              = errors.New("use of closed network connection")
	EMPTY_PAYLOAD                  = errors.New("empty payload")
	SERVER_TIMEOUT                 = errors.New("server time out")
	REQUEST_FAILED                 = errors.New("request failed")
	NOT_SUPPORTED_CMD              = errors.New("not supported command")
	DICT_IS_NIL                    = errors.New("dict is nil")
	SHARD_IS_NIL                   = errors.New("shard is nil")
	MIN_OR_MAX_IS_NOT_A_FLOAT      = errors.New("minimum or min value is not a float")
	MIN_OR_MAX_IS_NOT_VALID_STRING = errors.New("min or max not valid string range item")
	LIST_IS_NIL                    = errors.New("list is nil")
	INDEX_OUT_OF_RANGE             = errors.New("index out of range")
)
