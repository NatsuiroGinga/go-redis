package reply

import (
	"go-redis/lib/logger"
	"testing"
)

func TestNewBulkReply(t *testing.T) {
	reply := NewBulkReply([]byte("hello"))
	logger.Info("reply:", string(reply.Bytes()))
}

func TestNewMultiBulkReply(t *testing.T) {
	reply := NewMultiBulkReply([][]byte{[]byte("hello"), []byte("world")})
	logger.Info("reply:", string(reply.Bytes()))
}
