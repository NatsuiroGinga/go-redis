package reply

import (
	"go-redis/interface/resp"
	"strings"
	"sync"
)

var errorReplies = map[resp.ErrorReply][]byte{}

var (
	storeUnknownErrReplyOnce   sync.Once
	storeSyntaxErrReplyOnce    sync.Once
	storeWrongTypeErrReplyOnce sync.Once
)

var (
	theUnknownErrReply   *unknownErrReply
	theSyntaxErrReply    *syntaxErrReply
	theWrongTypeErrReply *wrongTypeErrReply
)

// unknownErrReply 用于表示未知错误的回复
type unknownErrReply struct {
}

func NewUnknownErrReply() resp.ErrorReply {
	storeUnknownErrReplyOnce.Do(func() {
		theUnknownErrReply = new(unknownErrReply)
		errorReplies[theUnknownErrReply] = []byte("-ERR unknown\r\n")
	})
	return theUnknownErrReply
}

func (reply *unknownErrReply) Bytes() []byte {
	return errorReplies[reply]
}

func (reply *unknownErrReply) Error() string {
	return strings.Trim(string(errorReplies[reply]), "-\r\n")
}

// argNumErrReply 用于表示参数数量错误的回复
type argNumErrReply struct {
	cmd string
}

func NewArgNumErrReply(cmd string) resp.ErrorReply {
	return &argNumErrReply{cmd}
}

func (reply *argNumErrReply) Bytes() []byte {
	return []byte("-ERR wrong number of arguments for '" + reply.cmd + "' command\r\n")
}

func (reply *argNumErrReply) Error() string {
	return "ERR wrong number of arguments for '" + reply.cmd + "' command"
}

// syntaxErrReply 用于表示语法错误的回复
type syntaxErrReply struct {
}

func NewSyntaxErrReply() resp.ErrorReply {
	storeSyntaxErrReplyOnce.Do(func() {
		theSyntaxErrReply = new(syntaxErrReply)
		errorReplies[theSyntaxErrReply] = []byte("-ERR syntax error\r\n")
	})
	return theSyntaxErrReply
}

func (reply *syntaxErrReply) Bytes() []byte {
	return errorReplies[reply]
}

func (reply *syntaxErrReply) Error() string {
	return strings.Trim(string(errorReplies[reply]), "-\r\n")
}

// wrongTypeErrReply 用于表示类型错误的回复
type wrongTypeErrReply struct {
}

func (reply *wrongTypeErrReply) Bytes() []byte {
	return errorReplies[reply]
}

func (reply *wrongTypeErrReply) Error() string {
	return strings.Trim(string(errorReplies[theWrongTypeErrReply]), "-\r\n")
}

func NewWrongTypeErrReply() resp.ErrorReply {
	storeWrongTypeErrReplyOnce.Do(func() {
		theWrongTypeErrReply = new(wrongTypeErrReply)
		errorReplies[theWrongTypeErrReply] = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	})
	return theWrongTypeErrReply
}

// protocolErrReply 用于表示协议错误的回复
type protocolErrReply struct {
	msg string
}

func (reply *protocolErrReply) Bytes() []byte {
	return []byte("-ERR Protocol error: '" + reply.msg + "'\r\n")
}

func (reply *protocolErrReply) Error() string {
	return "ERR Protocol error: '" + reply.msg + "'"
}

func NewProtocolErrReply(msg string) resp.ErrorReply {
	return &protocolErrReply{msg}
}
