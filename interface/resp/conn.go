package resp

import "io"

// Connection is an interface that represents a connection to a client.
// io.Writer is used to write data to the client.
// GetDBIndex returns the current db index.
// SelectDB selects the db with the given index.
type Connection interface {
	io.Writer
	GetDBIndex() int
	SelectDB(int)
}
