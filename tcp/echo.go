package tcp

import (
	"bufio"
	"context"
	"go-redis/lib/logger"
	"go-redis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait
}

func (client *EchoClient) Close() error {
	if client.Waiting.WaitWithTimeout(time.Second * 10) {
		logger.Error("client close timeout")
	} // wait for 10 seconds

	return client.Conn.Close()
}

// EchoHandler is a tcp handler that echo the message.
type EchoHandler struct {
	// activeConn is a map that stores the active connections.
	activeConn sync.Map
	// closed is a flag that indicates whether the handler is closed.
	closing atomic.Bool
}

func NewEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

func (handler *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if handler.closing.Load() { // if handler is closing, close the connection
		_ = conn.Close()
		return
	}
	// create a new client
	client := &EchoClient{Conn: conn}
	handler.activeConn.Store(client, struct{}{})
	reader := bufio.NewReader(conn)

	for {
		msg, err := reader.ReadString('\n')
		// handle the error
		if err != nil {
			if err == io.EOF { // if client closed, close the connection
				logger.Info("client closed")
				handler.activeConn.Delete(client)
			} else {
				logger.Warn("read error:", err)
			}

			return
		}
		// add waiting
		client.Waiting.Add(1)
		// write the message back to client
		b := []byte("response : " + msg)
		_, _ = conn.Write(b)
		// done waiting
		client.Waiting.Done()
	}
}

func (handler *EchoHandler) Close() error {
	var err error
	logger.Info("handler closing...")
	// set the closing flag
	handler.closing.Store(true)
	// close all the active connections
	handler.activeConn.Range(func(key, value any) bool {
		client := key.(*EchoClient)
		err = client.Close()

		if err != nil {
			logger.Error("close error:", err)
			return false
		}

		return true
	})

	return err
}
