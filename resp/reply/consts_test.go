package reply

import (
	"log"
	"testing"
)

func TestNewEmptyMultiBulkReply(t *testing.T) {
	er := NewEmptyMultiBulkReply()
	log.Println(string(er.Bytes()))
}

func TestNewNullBulkReply(t *testing.T) {
	nr := NewNullBulkReply()
	log.Println(string(nr.Bytes()))
}
