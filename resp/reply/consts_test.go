package reply

import (
	"log"
	"sync"
	"testing"
)

func TestNewEmptyMultiBulkReply(t *testing.T) {
	var mp sync.Map
	mp.Store("hello", "world")
	mp.Store("ping", "pong")
	mp.Store("foo", "bar")
	for i := 0; i < 3; i++ {
		mp.Range(func(key, value any) bool {
			log.Println(key, value)
			return false
		})
	}
}

func TestNewNullBulkReply(t *testing.T) {
	nr := NewNullBulkReply()
	log.Println(string(nr.Bytes()))
}
