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
	ERR_SYNTAX          = "-ERR syntax error" + CRLF
	ERR_UNKNOWN         = "-ERR unknown command" + CRLF
	ERR_ARG_NUM         = "-ERR wrong number of arguments for '%s' command" + CRLF
	ERR_UNKNOWN_CMD     = "-ERR unknown command '%s'" + CRLF
	ERR_STANDARD        = "-ERR %s" + CRLF
	ERR_WRONG_TYPE      = "-ERR Operation against a key holding the wrong kind of value" + CRLF
	ERR_PROTOCOL        = "-ERR Protocol error: '%s'" + CRLF
	ERR_INT             = "-ERR value is not an integer or out of range" + CRLF
	ERR_NO_SUCH_KEY     = "-ERR no such key" + CRLF
	ERR_NOT_VALID_FLOAT = "-ERR value is not a valid float" + CRLF
)
