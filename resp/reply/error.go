package reply

import (
	"bytes"
	"fmt"
	"strings"

	"go-redis/enum"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
)

/***************************************unknownErrReply*******************************************/
// unknownErrReply 用于表示未知错误的回复
type unknownErrReply struct {
}

// NewUnknownErrReply 用于创建未知错误的回复
func NewUnknownErrReply() resp.ErrorReply {
	return &unknownErrReply{}
}

func (reply *unknownErrReply) Bytes() []byte {
	return utils.String2Bytes(enum.ERR_UNKNOWN)
}

func (reply *unknownErrReply) Error() string {
	return bytes2Error(reply.Bytes())
}

/*****************************************argNumErrReply*****************************************/
// argNumErrReply 用于表示参数数量错误的回复
type argNumErrReply struct {
	cmd string // 表示命令
}

// NewArgNumErrReply 用于创建参数数量错误的回复
func NewArgNumErrReply(cmd string) resp.ErrorReply {
	return &argNumErrReply{cmd}
}

func NewArgNumErrReplyByCmd(cmd *enum.Command) resp.ErrorReply {
	return NewArgNumErrReply(cmd.String())
}

func (reply *argNumErrReply) Bytes() []byte {
	return utils.String2Bytes(fmt.Sprintf(enum.ERR_ARG_NUM, strings.ToLower(reply.cmd)))
}

func (reply *argNumErrReply) Error() string {
	return bytes2Error(reply.Bytes())
}

/*********************************************syntaxErrReply*************************************/
// syntaxErrReply 用于表示语法错误的回复
type syntaxErrReply struct {
}

// NewSyntaxErrReply 用于创建语法错误的回复
func NewSyntaxErrReply() resp.ErrorReply {
	return &syntaxErrReply{}
}

func (reply *syntaxErrReply) Bytes() []byte {
	return utils.String2Bytes(enum.ERR_SYNTAX)
}

func (reply *syntaxErrReply) Error() string {
	return bytes2Error(reply.Bytes())
}

/*****************************************wrongTypeErrReply*****************************************/
// wrongTypeErrReply 用于表示类型错误的回复
type wrongTypeErrReply struct{}

func (reply *wrongTypeErrReply) Bytes() []byte {
	return utils.String2Bytes(enum.ERR_WRONG_TYPE)
}

func (reply *wrongTypeErrReply) Error() string {
	return bytes2Error(reply.Bytes())
}

func NewWrongTypeErrReply() resp.ErrorReply {
	return &wrongTypeErrReply{}
}

/***************************************protocolErrReply*******************************************/
// protocolErrReply 用于表示协议错误的回复
type protocolErrReply struct {
	msg string // 表示错误信息
}

func (reply *protocolErrReply) Bytes() []byte {
	return utils.String2Bytes(fmt.Sprintf(enum.ERR_PROTOCOL, reply.msg))
}

func (reply *protocolErrReply) Error() string {
	return bytes2Error(reply.Bytes())
}

// NewProtocolErrReply 用于创建协议错误的回复
func NewProtocolErrReply(msg string) resp.ErrorReply {
	return &protocolErrReply{msg}
}

/***************************************standardErrReply*******************************************/
// standardErrReply 用于表示标准错误回复
type standardErrReply struct {
	status string // 表示错误状态
}

func (reply *standardErrReply) Error() string {
	return bytes2Error(reply.Bytes())
}

// Bytes 用于返回标准错误回复的字节切片
func (reply *standardErrReply) Bytes() []byte {
	return utils.String2Bytes(fmt.Sprintf(enum.ERR_STANDARD, reply.status))
}

// NewErrReply 用于创建标准错误回复
func NewErrReply(status string) resp.Reply {
	return &standardErrReply{status}
}

func NewErrReplyByError(err error) resp.Reply {
	return NewErrReply(err.Error())
}

// NormalErrReply 是自动添加 `-` 前缀和 `\r\n`后缀
type NormalErrReply struct {
	Status string
}

func (reply *NormalErrReply) Bytes() []byte {
	return utils.String2Bytes(fmt.Sprintf("-%s\r\n", reply.Status))
}

func (reply *NormalErrReply) Error() string {
	return reply.Status
}

/***************************************unknownCommandErrReply*******************************************/
// unknownCommandErrReply 用于表示未知命令的回复
type unknownCommandErrReply struct {
	cmd string // 表示命令
}

// NewUnknownCommandErrReply 用于创建未知命令的回复
func NewUnknownCommandErrReply(cmd string) resp.ErrorReply {
	return &unknownCommandErrReply{cmd}
}

func (reply *unknownCommandErrReply) Bytes() []byte {
	return utils.String2Bytes(fmt.Sprintf(enum.ERR_UNKNOWN_CMD, reply.cmd))
}

func (reply *unknownCommandErrReply) Error() string {
	return bytes2Error(reply.Bytes())
}

/***************************************intErrReply*******************************************/
// intErrReply 用于表示整数类型错误或者超过整数范围
type intErrReply struct {
}

func (reply *intErrReply) Bytes() []byte {
	return utils.String2Bytes(enum.ERR_INT)
}

func (reply *intErrReply) Error() string {
	return bytes2Error(reply.Bytes())
}

// NewIntErrReply 用于创建整数错误的回复
func NewIntErrReply() resp.ErrorReply {
	return &intErrReply{}
}

/***************************************noSuchKeyErrReply*******************************************/
type noSuchKeyErrReply struct{}

func (reply *noSuchKeyErrReply) Bytes() []byte {
	return utils.String2Bytes(enum.ERR_NO_SUCH_KEY)
}

func (reply *noSuchKeyErrReply) Error() string {
	return bytes2Error(reply.Bytes())
}

func NewNoSuchKeyErrReply() resp.ErrorReply {
	return &noSuchKeyErrReply{}
}

/***************************************notValidFloatErrReply*******************************************/
type notValidFloatErrReply struct{}

func NewNotValidFloatErrReply() resp.ErrorReply {
	return &notValidFloatErrReply{}
}

func (reply *notValidFloatErrReply) Bytes() []byte {
	return utils.String2Bytes(enum.ERR_NOT_VALID_FLOAT)
}

func (reply *notValidFloatErrReply) Error() string {
	return bytes2Error(reply.Bytes())
}

// bytes2Error 用于将字节切片转换为字符串, 同时去除前缀'-'和后缀'\r\n'
func bytes2Error(b []byte) string {
	return utils.Bytes2String(bytes.Trim(b, "-\r\n"))
}
