package reply

import (
	"bytes"
	"fmt"
	"go-redis/interface/resp"
)

var (
	CRLF               = "\r\n"
	nullBulkReplyBytes = []byte("$-1")
)

// BulkReply 用于表示回复字符串
type bulkReply struct {
	Arg []byte
}

func NewBulkReply(arg []byte) resp.Reply {
	return &bulkReply{arg}
}

func (reply *bulkReply) Bytes() []byte {
	if len(reply.Arg) == 0 { // 如果是空字符串, 则返回空字符串的回复
		return []byte(fmt.Sprintf("%s%s", nullBulkReplyBytes, CRLF))
	}
	return []byte(fmt.Sprintf("$%d%s%s%s", len(reply.Arg), CRLF, reply.Arg, CRLF))
}

// MultiBulkReply 用于表示回复数组
type multiBulkReply struct {
	Args [][]byte
}

func (reply *multiBulkReply) Bytes() []byte {
	if len(reply.Args) == 0 { // 如果是空数组, 则返回空数组的回复
		return []byte("*0" + CRLF)
	}

	argLen := len(reply.Args)
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("*%d%s", argLen, CRLF))

	for _, arg := range reply.Args {
		if len(arg) == 0 { // 如果数组中有空字符串, 则返回空字符串的回复
			buf.Write(nullBulkReplyBytes)
			buf.WriteString(CRLF)
		} else {
			buf.WriteString(fmt.Sprintf("$%d%s%s%s", len(arg), CRLF, arg, CRLF))
		}
	}

	return buf.Bytes()
}

func NewMultiBulkReply(args [][]byte) resp.Reply {
	return &multiBulkReply{args}
}
