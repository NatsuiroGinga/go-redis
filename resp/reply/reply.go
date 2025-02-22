package reply

import (
	"bytes"
	"fmt"
	"strconv"

	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
)

// BulkReply 用于表示回复字符串
type BulkReply struct {
	Arg []byte // 表示原始命令
}

// NewBulkReply 用于创建回复字符串
func NewBulkReply(arg []byte) *BulkReply {
	return &BulkReply{arg}
}

func (reply *BulkReply) Bytes() []byte {
	if len(reply.Arg) == 0 { // 如果是空字符串, 则返回空字符串的回复
		return utils.String2Bytes(enum.NIL)
	}
	return utils.String2Bytes(fmt.Sprintf("$%d%s%s%s", len(reply.Arg), enum.CRLF, reply.Arg, enum.CRLF))
}

/**********************************************************************************************************************/

// MultiBulkReply 用于表示回复数组
type MultiBulkReply struct {
	Args db.CmdLine // 表示数组中的元素
}

func (reply *MultiBulkReply) Bytes() []byte {
	if len(reply.Args) == 0 { // 如果是空数组, 则返回空数组的回复
		return utils.String2Bytes(enum.EMPTY_BULK_REPLY)
	}
	var buf bytes.Buffer
	// Calculate the length of buffer
	argLen := len(reply.Args)
	bufLen := 1 + len(strconv.Itoa(argLen)) + 2
	for _, arg := range reply.Args {
		if arg == nil {
			bufLen += 3 + 2
		} else {
			bufLen += 1 + len(strconv.Itoa(len(arg))) + 2 + len(arg) + 2
		}
	}
	// Allocate memory
	buf.Grow(bufLen)
	buf.WriteString(fmt.Sprintf("*%d%s", argLen, enum.CRLF))

	for _, arg := range reply.Args {
		if len(arg) == 0 { // 如果数组中有空字符串, 则返回空字符串的回复
			buf.WriteString(enum.NIL)
		} else {
			buf.WriteString(fmt.Sprintf("$%d%s", len(arg), enum.CRLF))
			buf.Write(arg)
			buf.WriteString(enum.CRLF)
		}
	}

	return buf.Bytes()
}

// NewMultiBulkReply 用于创建回复数组
func NewMultiBulkReply(args db.CmdLine) *MultiBulkReply {
	return &MultiBulkReply{args}
}

/**********************************************************************************************************************/

// StatusReply 用于表示回复状态
type StatusReply struct {
	status string // 表示状态值
}

func (reply *StatusReply) Status() string {
	return reply.status
}

func (reply *StatusReply) Bytes() []byte {
	return utils.String2Bytes(fmt.Sprintf("+%s%s", reply.status, enum.CRLF))
}

// NewStatusReply 用于创建回复状态
func NewStatusReply(status string) resp.Reply {
	return &StatusReply{status}
}

/**********************************************************************************************************************/

// IntReply 用于表示回复整数
type IntReply struct {
	code int64 // 表示整数值
}

func (reply *IntReply) Code() int64 {
	return reply.code
}

// Bytes 用于返回回复整数的字节切片
func (reply *IntReply) Bytes() []byte {
	return utils.String2Bytes(fmt.Sprintf(":%d%s", reply.code, enum.CRLF))
}

// NewIntReply 用于创建回复整数
func NewIntReply(code int64) resp.Reply {
	return &IntReply{code}
}

// IsErrReply 用于判断回复是否是错误回复
func IsErrReply(reply resp.Reply) bool {
	_, ok := reply.(resp.ErrorReply)
	return ok
}

/**********************************************************************************************************************/

// MultiRawReply store complex list structure, for example GeoPos command
type MultiRawReply struct {
	Replies []resp.Reply
}

// NewMultiRawReply creates MultiRawReply
func NewMultiRawReply(replies []resp.Reply) *MultiRawReply {
	return &MultiRawReply{
		Replies: replies,
	}
}

// Bytes marshal redis.Reply
func (r *MultiRawReply) Bytes() []byte {
	argLen := len(r.Replies)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + enum.CRLF)
	for _, re := range r.Replies {
		buf.Write(re.Bytes())
	}
	return buf.Bytes()
}
