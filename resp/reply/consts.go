package reply

import (
	"go-redis/enum"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
)

// 用于存储所有的回复, 使用懒加载的方式, 只有在需要的时候才会初始化且只会初始化一次
var replies map[resp.Reply][]byte

// 优化: 使用单例模式, 保证只有一个实例, 且只有在需要的时候才会初始化
var (
	thePongReply           *PongReply
	theOKReply             *okReply
	theNullBulkReply       *NullBulkReply
	theEmptyMultiBulkReply *emptyMultiBulkReply
	theNoReply             *noReply
	theQueuedReply         *queuedReply
)

func init() {
	theNoReply = new(noReply)
	theEmptyMultiBulkReply = new(emptyMultiBulkReply)
	thePongReply = new(PongReply)
	theOKReply = new(okReply)
	theNullBulkReply = new(NullBulkReply)
	theQueuedReply = new(queuedReply)

	replies = map[resp.Reply][]byte{
		theNoReply:             utils.String2Bytes(enum.NO_REPLY),
		theEmptyMultiBulkReply: utils.String2Bytes(enum.EMPTY_BULK_REPLY),
		thePongReply:           utils.String2Bytes(enum.PONG),
		theOKReply:             utils.String2Bytes(enum.OK),
		theNullBulkReply:       utils.String2Bytes(enum.NIL),
		theQueuedReply:         queuedBytes,
	}
}

// PongReply 用于表示PONG的回复
type PongReply struct {
}

func NewPongReply() resp.Reply {
	return thePongReply
}

func (reply *PongReply) Bytes() []byte {
	return replies[reply]
}

// OKReply 用于表示OK的回复
type okReply struct {
}

// NewOKReply 用于创建OK的回复
func NewOKReply() resp.Reply {
	return theOKReply
}

func (reply *okReply) Bytes() []byte {
	return replies[reply]
}

// nullBulkReply 用于表示空的回复字符串
type NullBulkReply struct {
}

// NewNullBulkReply 用于创建空的回复字符串
func NewNullBulkReply() resp.Reply {
	return theNullBulkReply
}

func (reply *NullBulkReply) Bytes() []byte {
	return replies[reply]
}

// emptyMultiBulkReply 用于表示空的多条批量回复数组
type emptyMultiBulkReply struct {
}

// NewEmptyMultiBulkReply 用于创建空的多条批量回复数组
func NewEmptyMultiBulkReply() resp.Reply {
	return theEmptyMultiBulkReply
}

func (reply *emptyMultiBulkReply) Bytes() []byte {
	return replies[reply]
}

// noReply 用于表示没有回复
type noReply struct {
}

func NewNoReply() resp.Reply {
	return theNoReply
}

func (reply *noReply) Bytes() []byte {
	return replies[reply]
}

type queuedReply struct{}

func (reply *queuedReply) Bytes() []byte {
	return queuedBytes
}

var queuedBytes = []byte("+QUEUED\r\n")

// NewQueuedReply returns a QUEUED protocol
func NewQueuedReply() resp.Reply {
	return theQueuedReply
}
