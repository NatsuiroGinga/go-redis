package tcp

import (
	"context"
	"net"
)

// Handler is the interface that must be implemented by a handler.
type Handler interface {
	// Handle is called when a new connection is accepted.
	Handle(ctx context.Context, conn net.Conn)
	// Close is called when the server is closed.
	Close() error
}
