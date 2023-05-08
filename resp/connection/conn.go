package connection

import (
	"go-redis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// RespConnection is the connection to the client.
type RespConnection struct {
	conn         net.Conn   // the connection to the client
	waitingReply wait.Wait  // the waiting reply
	mu           sync.Mutex // the mutex to protect the connection
	selectedDB   int        // the selected db index
}

func NewRespConnection(conn net.Conn) *RespConnection {
	return &RespConnection{conn: conn}
}

// RemoteAddr returns the remote network address.
func (rc *RespConnection) RemoteAddr() net.Addr {
	return rc.conn.RemoteAddr()
}

// Close closes the connection.
func (rc *RespConnection) Close() error {
	rc.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = rc.conn.Close()
	return nil
}

// Write writes data to the connection and returns the number of bytes written and an error if any.
//
// If len(p) == 0, Write returns 0, nil without writing anything.
//
// Mutex is used to protect the connection.
func (rc *RespConnection) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	rc.mu.Lock()
	rc.waitingReply.Add(1)
	defer func() {
		rc.waitingReply.Done()
		rc.mu.Unlock()
	}()

	return rc.conn.Write(p)
}

// GetDBIndex returns the selected db index.
func (rc *RespConnection) GetDBIndex() int {
	return rc.selectedDB
}

// SelectDB selects the db by the given index.
func (rc *RespConnection) SelectDB(dbIndex int) {
	rc.selectedDB = dbIndex
}
