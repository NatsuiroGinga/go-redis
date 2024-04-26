package resp

import (
	"io"
)

// Connection is an interface that represents a connection to a client.
// io.Writer is used to write data to the client.
// GetDBIndex returns the current db index.
// SelectDB selects the db with the given index.
type Connection interface {
	io.Writer
	io.Closer
	GetDBIndex() int
	SelectDB(int)
	RemoteAddr() string

	// password
	SetPassword(string)
	GetPassword() string

	// transaction
	InMultiState() bool
	SetMultiState(bool)
	GetQueuedCmdLine() [][][]byte
	EnqueueCmd([][]byte)
	ClearQueuedCmds()
	GetWatching() map[string]uint32
	ClearWatching()
	AddTxError(err error)
	GetTxErrors() []error
}
