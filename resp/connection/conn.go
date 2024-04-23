package connection

import (
	"net"
	"sync"
	"time"

	"go-redis/interface/db"
	"go-redis/lib/sync/wait"
)

const (
	// flagSlave means this a connection with slave, 001
	flagSlave = uint64(1 << iota)
	// flagSlave means this a connection with master, 010
	flagMaster
	// flagMulti means this connection is within a transaction, 100
	flagMulti
)

// RespConnection is the connection to the client.
type RespConnection struct {
	conn         net.Conn   // the connection to the client
	waitingReply wait.Wait  // the waiting reply
	mu           sync.Mutex // the mutex to protect the connection
	flags        uint64
	selectedDB   int // the selected db index

	// password is user's password
	password string

	// implement transaction
	queue             []db.CmdLine      // 事务命令的执行队列
	watching          map[string]uint32 // 一个事务执行过程中的有关的键与对应的版本号
	transactionErrors []error           // 事务执行中的抛出的错误
}

func (rc *RespConnection) GetPassword() string {
	return rc.password
}

func (rc *RespConnection) InMultiState() bool {
	return rc.flags&flagMulti > 0
}

func (rc *RespConnection) GetQueuedCmdLine() []db.CmdLine {
	return rc.queue
}

func (rc *RespConnection) SetPassword(password string) {
	rc.password = password
}

// SetMultiState 设置此链接正在执行事务的标志
//
// 如果设置为false, 则会清空watching, queue
func (rc *RespConnection) SetMultiState(state bool) {
	if !state { // reset data when cancel multi
		rc.watching = nil
		rc.queue = nil
		rc.flags &= ^flagMulti // clean multi flag
		return
	}
	rc.flags |= flagMulti
}

// EnqueueCmd  enqueues command of current transaction
func (rc *RespConnection) EnqueueCmd(cmdLine db.CmdLine) {
	rc.queue = append(rc.queue, cmdLine)
}

// AddTxError stores syntax error within transaction
func (rc *RespConnection) AddTxError(err error) {
	rc.transactionErrors = append(rc.transactionErrors, err)
}

// TxErrors returns syntax error within transaction
func (rc *RespConnection) GetTxErrors() []error {
	return rc.transactionErrors
}

// ClearCmdQueue clears queued commands of current transaction
func (rc *RespConnection) ClearQueuedCmds() {
	rc.queue = nil
}

func NewRespConnection(conn net.Conn) *RespConnection {
	return &RespConnection{conn: conn}
}

// RemoteAddr returns the remote network address.
func (rc *RespConnection) RemoteAddr() string {
	return rc.conn.RemoteAddr().String()
}

// Watching returns watching keys and their version code when started watching
func (rc *RespConnection) GetWatching() map[string]uint32 {
	if rc.watching == nil {
		rc.watching = make(map[string]uint32)
	}
	return rc.watching
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
