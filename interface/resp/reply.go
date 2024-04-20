package resp

// Reply 是一个给客户端回复的接口
type Reply interface {
	// Bytes 返回一个字节数组的回复, 使用resp格式
	Bytes() []byte
}

// ErrorReply 是一个给客户端错误回复的接口
type ErrorReply interface {
	Reply
	// Error 只返回错误信息, 不使用resp格式
	Error() string
}
