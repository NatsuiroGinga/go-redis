package resp

import "io"

// Connection is an interface that represents a connection to a client.
// io.Writer is used to write data to the client.
// GetDBIndex returns the current database index.
// SelectDB selects the database with the given index.
type Connection interface {
	io.Writer
	GetDBIndex() int
	SelectDB(int)
}
