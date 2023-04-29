package reply

import (
	"go-redis/interface/resp"
	"sync"
)

// 用于存储所有的回复, 使用懒加载的方式, 只有在需要的时候才会初始化且只会初始化一次
var replies = map[resp.Reply][]byte{}

// 用于保证只初始化一次
var (
	storePongReplyOnce           sync.Once
	storeOKReplyOnce             sync.Once
	storeNullBulkReplyOnce       sync.Once
	storeEmptyMultiBulkReplyOnce sync.Once
	storeNoReplyOnce             sync.Once
)

// 优化: 使用单例模式, 保证只有一个实例, 且只有在需要的时候才会初始化
var (
	thePongReply           *pongReply
	theOKReply             *okReply
	theNullBulkReply       *nullBulkReply
	theEmptyMultiBulkReply *emptyMultiBulkReply
	theNoReply             *noReply
)

// PongReply 用于表示PONG的回复
type pongReply struct {
}

func NewPongReply() resp.Reply {
	storePongReplyOnce.Do(func() {
		thePongReply = new(pongReply)
		replies[thePongReply] = []byte("+PONG\r\n")
	})
	return thePongReply
}

func (reply *pongReply) Bytes() []byte {
	return replies[reply]
}

// OKReply 用于表示OK的回复
type okReply struct {
}

func NewOKReply() resp.Reply {
	storeOKReplyOnce.Do(func() {
		theOKReply = new(okReply)
		replies[theOKReply] = []byte("+OK\r\n")
	})
	return theOKReply
}

func (reply *okReply) Bytes() []byte {
	return replies[reply]
}

// nullBulkReply 用于表示空的回复字符串
type nullBulkReply struct {
}

func NewNullBulkReply() resp.Reply {
	storeNullBulkReplyOnce.Do(func() {
		theNullBulkReply = new(nullBulkReply)
		replies[theNullBulkReply] = []byte("$-1\r\n")
	})
	return theNullBulkReply
}

func (reply *nullBulkReply) Bytes() []byte {
	return replies[reply]
}

// emptyMultiBulkReply 用于表示空的多条批量回复数组
type emptyMultiBulkReply struct {
}

func NewEmptyMultiBulkReply() resp.Reply {
	storeEmptyMultiBulkReplyOnce.Do(func() {
		theEmptyMultiBulkReply = new(emptyMultiBulkReply)
		replies[theEmptyMultiBulkReply] = []byte("*0\r\n")
	})
	return theEmptyMultiBulkReply
}

func (reply *emptyMultiBulkReply) Bytes() []byte {
	return replies[reply]
}

// noReply 用于表示没有回复
type noReply struct {
}

func NewNoReply() resp.Reply {
	storeNoReplyOnce.Do(func() {
		theNoReply = new(noReply)
		replies[theNoReply] = []byte("")
	})
	return theNoReply
}

func (reply *noReply) Bytes() []byte {
	return replies[reply]
}
