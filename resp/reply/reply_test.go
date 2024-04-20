package reply

import (
	"testing"

	"go-redis/lib/logger"
)

func TestNewBulkReply(t *testing.T) {
	reply := NewBulkReply([]byte("hello"))
	logger.Info("reply:", string(reply.Bytes()))
}

func TestNewMultiBulkReply(t *testing.T) {
	reply := NewMultiBulkReply([][]byte{[]byte("ping"), []byte(" "), []byte("pong")})
	logger.Info("reply:", string(reply.Bytes()))
}

func TestIsErrReply(t *testing.T) {
	reply := NewUnknownErrReply()
	logger.Info("reply:", string(reply.Bytes()))

	if IsErrReply(reply) {
		logger.Info("reply is error reply")
	} else {
		logger.Info("reply is not error reply")
	}

	reply2 := NewBulkReply([]byte("hello"))
	logger.Info("reply:", string(reply2.Bytes()))

	if IsErrReply(reply2) {
		logger.Info("reply2 is error reply")
	} else {
		logger.Info("reply2 is not error reply")
	}
}
