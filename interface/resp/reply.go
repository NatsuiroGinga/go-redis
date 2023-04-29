package resp

// Reply is an interface that represents a reply to a client.
// Bytes returns the reply as a byte slice.
type Reply interface {
	Bytes() []byte
}

// ErrorReply is an interface that represents an error reply to a client.
// Error returns the error message.
type ErrorReply interface {
	Reply
	Error() string
}
