package enum

const CRLF = "\r\n"

// 固定回复
const (
	PONG             = "+PONG" + CRLF
	OK               = "+OK" + CRLF
	NIL              = "$-1" + CRLF
	EMPTY_BULK_REPLY = "*0" + CRLF
	NO_REPLY         = "" + CRLF
)

// 错误回复
const (
	SYNTAX_ERR      = "-ERR syntax error" + CRLF
	UNKNOWN_ERR     = "-ERR unknown command" + CRLF
	ARG_NUM_ERR     = "-ERR wrong number of arguments for '%s' command" + CRLF
	UNKNOWN_CMD_ERR = "-ERR unknown command '%s'" + CRLF
	STANDARD_ERR    = "-%s" + CRLF
	WRONG_TYPE_ERR  = "-WRONGTYPE Operation against a key holding the wrong kind of value" + CRLF
	PROTOCOL_ERR    = "-ERR Protocol error: '%s'" + CRLF
	INT_ERR         = "-ERR value is not an integer or out of range" + CRLF
)
