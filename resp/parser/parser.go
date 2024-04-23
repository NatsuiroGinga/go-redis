package parser

import (
	"bufio"
	"io"
	"runtime/debug"
	"strconv"
	"strings"

	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// Payload 用于表示解析后的数据
type Payload struct {
	Data resp.Reply // 表示解析后的数据
	Err  error      // 表示解析过程中的错误
}

// readState 用于表示读取状态
type readState struct {
	readingMultiline  bool       // 表示是否正在读取多行数据
	expectedArgsCount int        // 表示期望的参数个数
	msgType           byte       // 表示消息类型
	args              db.CmdLine // 表示已解析的参数
	bulkLen           int64      // 表示一行数据的长度
}

// finished 用于判断是否已经读取完毕
//
// 如果期望的参数个数大于0, 且已经读取的参数个数等于期望的参数个数, 则表示已经读取完毕
func (rs *readState) finished() bool {
	return rs.expectedArgsCount > 0 && len(rs.args) == rs.expectedArgsCount
}

// ParseStream 用于解析RESP协议, 并返回解析后的数据
//
// 该函数会启动一个goroutine, 用于解析RESP协议
//
// 返回一个通道, 该通道会返回解析后的数据
func ParseStream(rd io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(rd, ch)
	return ch
}

// parse0 用于解析RESP协议, 并将解析后的数据发送到通道中
func parse0(r io.Reader, ch chan<- *Payload) {
	defer func() { // 如果解析过程中发生异常
		if err := recover(); err != nil { // 捕获异常
			logger.Error(utils.Bytes2String(debug.Stack()))
		}
	}()

	br := bufio.NewReader(r)

	var (
		rs  readState
		err error
		msg []byte
	)

	for { // 循环读取数据
		var hasIOErr bool
		if msg, hasIOErr, err = readLine(br, &rs); err != nil { // 读取一行数据
			if hasIOErr { // 如果是IO错误, 则关闭通道, 并退出循环
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
			ch <- &Payload{Err: err} // 如果是协议错误, 则发送错误信息到通道中
			rs = readState{}         // 重置读取状态
			continue
		}

		if len(msg) == 0 || msg[0] == '\n' || msg[0] == '\r' { // 如果读取到的数据为空或者是空行, 则继续读取
			continue
		}

		if !rs.readingMultiline { // 如果是初始读取或者不是处于多行数据读取状态
			if msg[0] == '*' { // 如果是多行数据的头部
				err = parseMultiBulkHeader(msg, &rs) // 解析多行数据的头部

				if err != nil {
					send(&rs, ch, err, nil)
					continue
				}
				if rs.expectedArgsCount == 0 { // 如果参数个数为0, 则发送空的多行数据到通道中
					send(&rs, ch, nil, reply.NewEmptyMultiBulkReply())
					continue
				}
			} else if msg[0] == '$' { // 如果是一行数据的头部
				err = parseBulkHeader(msg, &rs) // 解析一行数据的头部

				if err != nil {
					send(&rs, ch, err, nil)
					continue
				}
				if rs.bulkLen == -1 {
					send(&rs, ch, nil, reply.NewNullBulkReply())
					continue
				}
			} else { // 解析单行数据
				res, err := parseSingleLineReply(msg)
				send(&rs, ch, err, res)
				continue
			}
		} else {
			err = readBody(msg, &rs)
			if err != nil {
				send(&rs, ch, err, nil)
				continue
			}
			if rs.finished() {
				var result resp.Reply
				switch rs.msgType {
				case '*':
					result = reply.NewMultiBulkReply(rs.args)
				case '$':
					result = reply.NewBulkReply(rs.args[0])
				}
				send(&rs, ch, nil, result)
			}
		}
	}
}

// send 用于将解析后的数据发送到通道中
//
// rs 表示读取状态,
// ch 表示通道,
// err 表示错误,
// data 表示数据
func send(rs *readState, ch chan<- *Payload, err error, data resp.Reply) {
	ch <- &Payload{data, err}
	*rs = readState{}
}

// readLine 用于读取一行数据
//
// 如果没有$开头, 则表示以\r\n结尾读取一行数据
//
// 如果有$开头, 则表示严格按照bulkLen+2字节数读取数据
func readLine(br *bufio.Reader, rs *readState) (line []byte, hasIOErr bool, err error) {
	lineStr := utils.Bytes2String(line)

	if rs.bulkLen == 0 { // 如果没有$开头, 则表示以\r\n结尾读取一行数据
		line, err = br.ReadBytes('\n')
		if err == io.EOF {
			return nil, true, err
		}

		if err != nil {
			logger.Error("readLine error:", lineStr)
			return nil, true, err
		}

		if len(line) == 0 || line[len(line)-2] != '\r' { // 如果数据不以回车换行符结尾, 则返回错误
			logger.Error("readLine error:", lineStr)
			return nil, false, reply.NewProtocolErrReply(lineStr)
		}

	} else { // 如果有$开头, 则表示严格按照bulkLen+2字节数读取数据
		line = make([]byte, rs.bulkLen+2)
		_, err = io.ReadFull(br, line)

		if err != nil {
			return nil, true, err
		}

		if len(line) == 0 || line[len(line)-2] != '\r' || line[len(line)-1] != '\n' { // 如果数据不以回车换行符结尾, 则返回错误
			return nil, false, reply.NewProtocolErrReply(lineStr)
		}

		rs.bulkLen = 0 // 重置bulkLen
	}

	return line, false, nil
}

// parseMultiBulkHeader 用于解析多行数据的头部
//
// 例如: *3\r\n
//
//	$3\r\n
//	SET\r\n
//	$3\r\n
//	key\r\n
//	$5\r\n
//	value\r\n
func parseMultiBulkHeader(msg []byte, rs *readState) (err error) {
	rs.expectedArgsCount, err = strconv.Atoi(utils.Bytes2String(msg[1 : len(msg)-2]))

	msgStr := utils.Bytes2String(msg)
	if err != nil {
		return reply.NewProtocolErrReply(msgStr)
	}
	// err = nil
	if rs.expectedArgsCount > 0 { // 如果参数个数大于0
		rs.readingMultiline = true                          // 表示正在读取多行数据
		rs.msgType = msg[0]                                 // 保存消息类型
		rs.args = make(db.CmdLine, 0, rs.expectedArgsCount) // 初始化参数列表
	} else if rs.expectedArgsCount < 0 {
		rs.expectedArgsCount = 0
		err = reply.NewProtocolErrReply(msgStr)
	} // else rs.expectedArgsCount == 0, 不需要做操作, 调用此函数之后会做判断

	return err
}

// parseBulkHeader 用于解析一行数据的头部
//
// 例如: $4\r\nPING\r\n
func parseBulkHeader(msg []byte, rs *readState) (err error) {
	rs.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)

	msgStr := utils.Bytes2String(msg)
	if err != nil || rs.bulkLen < -1 {
		return reply.NewProtocolErrReply(msgStr)
	}
	// err = nil
	if rs.bulkLen > 0 { // 如果bulkLen大于0, 则表示需要读取bulkLen+2字节数据
		rs.msgType = msg[0]
		rs.readingMultiline = true
		rs.args = make(db.CmdLine, 0, 1)
		rs.expectedArgsCount = 1
	} else if rs.bulkLen != -1 {
		err = reply.NewProtocolErrReply(msgStr)
	} // else rs.bulkLen == -1, 不需要做操作, 调用此函数的之后会做判断

	return err
}

// parseSingleLineReply 用于解析单行回复
//
// 例如: +OK\r\n, -ERR\r\n, :1000\r\n
func parseSingleLineReply(msg []byte) (res resp.Reply, err error) {
	// 去除开头的标示和末尾的\r\n
	content := strings.TrimSuffix(utils.Bytes2String(msg), enum.CRLF)[1:]

	switch msg[0] {
	case '+': // 如果是+开头, 则表示是状态回复
		res = reply.NewStatusReply(content)
	case '-': // 如果是-开头, 则表示是错误回复
		res = reply.NewErrReply(strings.TrimPrefix(content, "ERR "))
	case ':': // 如果是:开头, 则表示是整数回复
		var code int64
		code, err = strconv.ParseInt(content, 10, 64)

		if err != nil { // 如果解析失败, 则返回错误
			return nil, reply.NewProtocolErrReply(content)
		}

		res = reply.NewIntReply(code)
	}

	return res, nil
}

// readBody 用于读取消息体
//
// 例如: $3\r\n
// 例如: PING\r\n
func readBody(msg []byte, rs *readState) (err error) {
	line := msg[:len(msg)-2] // 去掉\r\n

	if line[0] == '$' { // exp. line = $3
		rs.bulkLen, err = strconv.ParseInt(utils.Bytes2String(line[1:]), 10, 64)

		if err != nil {
			return reply.NewProtocolErrReply(utils.Bytes2String(line))
		}
		// 如果bulkLen小于0, 则表示是空值
		if rs.bulkLen <= 0 {
			rs.args = append(rs.args, []byte{})
			rs.bulkLen = 0
		}
	} else { // exp. line = PING
		rs.args = append(rs.args, line)
	}

	return nil
}
