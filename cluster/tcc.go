package cluster_database

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go-redis/database"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/lib/timewheel"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// prepareFunc executed after related key locked, and use additional logic to determine whether the transaction can be committed
// For example, prepareMSetNX  will return error to prevent MSe tNx transaction from committing if any related key already exists
var prepareFuncMap = make(map[string]execFunc)

func registerPrepareFunc(cmdName string, fn execFunc) {
	prepareFuncMap[strings.ToUpper(cmdName)] = fn
}

// Transaction stores state and data for a try-commit-catch distributed transaction
type Transaction struct {
	id      string     // transaction id
	cmdLine db.CmdLine // cmd cmdLine
	cluster *ClusterDatabase
	conn    resp.Connection
	dbIndex int

	writeKeys  []string
	readKeys   []string
	keysLocked bool
	undoLog    []db.CmdLine

	status int8
	mu     *sync.Mutex
}

const (
	maxLockTime       = 3 * time.Second
	waitBeforeCleanTx = 2 * maxLockTime

	createdStatus    = 0
	preparedStatus   = 1
	committedStatus  = 2
	rolledBackStatus = 3
)

func genTaskKey(txID string) string {
	return "tx:" + txID
}

// NewTransaction creates a try-commit-catch distributed transaction
func NewTransaction(cluster *ClusterDatabase, conn resp.Connection, id string, cmdLine db.CmdLine) *Transaction {
	return &Transaction{
		id:      id,
		cmdLine: cmdLine,
		cluster: cluster,
		conn:    conn,
		dbIndex: conn.GetDBIndex(),
		status:  createdStatus,
		mu:      new(sync.Mutex),
	}
}

// Reentrant
// invoker should hold tx.mu
func (tx *Transaction) lockKeys() {
	if !tx.keysLocked {
		tx.cluster.db.RWLocks(tx.dbIndex, tx.writeKeys, tx.readKeys)
		tx.keysLocked = true
	}
}

func (tx *Transaction) unLockKeys() {
	if tx.keysLocked {
		tx.cluster.db.RWUnLocks(tx.dbIndex, tx.writeKeys, tx.readKeys)
		tx.keysLocked = false
	}
}

// prepare 在做事务之前, 准备给要读写的键上锁
func (tx *Transaction) prepare() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.writeKeys, tx.readKeys = database.GetRelatedKeys(tx.cmdLine)
	// lock writeKeys
	tx.lockKeys()

	// build undoLog
	tx.undoLog = tx.cluster.db.GetUndoLogs(tx.dbIndex, tx.cmdLine)
	tx.status = preparedStatus
	taskKey := genTaskKey(tx.id)
	timewheel.Delay(maxLockTime, taskKey, func() {
		if tx.status == preparedStatus { // rollback transaction uncommitted until expire
			logger.Info("abort transaction: " + tx.id)
			tx.mu.Lock()
			defer tx.mu.Unlock()
			_ = tx.rollbackWithLock()
		}
	})
	return nil
}

func (tx *Transaction) commit() (result resp.Reply, err resp.ErrorReply) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	result = tx.cluster.db.ExecWithoutLock(tx.conn, tx.cmdLine)

	if reply.IsErrReply(result) {
		// failed
		err2 := tx.rollbackWithLock()
		return nil, reply.NewErrReply(fmt.Sprintf("occurs when rollback: %v, origin err: %s", err2, result))
	}
	// after committed
	tx.unLockKeys()
	tx.status = committedStatus
	// clean finished transaction
	// do not clean immediately, in case rollback
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		tx.cluster.txMutex.Lock()
		tx.cluster.transactions.Remove(tx.id)
		tx.cluster.txMutex.Unlock()
	})

	return result, nil
}

func (tx *Transaction) rollbackWithLock() error {
	curStatus := tx.status

	if tx.status != curStatus { // ensure status not changed by other goroutine
		return fmt.Errorf("tx %s status changed", tx.id)
	}
	if tx.status == rolledBackStatus { // no need to rollback a rolled-back transaction
		return nil
	}
	// 执行本地的undo log
	tx.lockKeys()
	for _, cmdLine := range tx.undoLog {
		tx.cluster.db.ExecWithoutLock(tx.conn, cmdLine)
	}
	tx.unLockKeys()
	tx.status = rolledBackStatus
	return nil
}

// cmdLine: Prepare id cmdName args...
func execPrepare(cluster *ClusterDatabase, conn resp.Connection, cmdLine db.CmdLine) resp.Reply {
	if len(cmdLine) < 3 { // args >= 3
		return reply.NewArgNumErrReply(enum.TCC_PREPARE.String())
	}
	// 1. 取出参数
	txID := utils.Bytes2String(cmdLine[1])
	cmdName := strings.ToUpper(utils.Bytes2String(cmdLine[2]))
	tx := NewTransaction(cluster, conn, txID, cmdLine[2:])
	// 2. 设置事务id
	cluster.txMutex.Lock()
	cluster.transactions.Set(txID, tx)
	cluster.txMutex.Unlock()
	// 3. 给相关的键上锁
	err := tx.prepare()
	if err != nil {
		return reply.NewErrReply(err.Error())
	}
	// 4. 可能有除了上锁之外的逻辑处理
	prepareFunc, ok := prepareFuncMap[cmdName]
	if ok {
		return prepareFunc(cluster, conn, cmdLine[2:])
	}
	return reply.NewOKReply()
}

// execRollback rollbacks local transaction
func execRollback(cluster *ClusterDatabase, _ resp.Connection, cmdLine db.CmdLine) resp.Reply {
	if len(cmdLine) != 2 {
		return reply.NewArgNumErrReply(enum.TCC_ROLLBACK.String())
	}
	txID := string(cmdLine[1])
	cluster.txMutex.RLock()
	raw, ok := cluster.transactions.Get(txID)
	cluster.txMutex.RUnlock()
	if !ok {
		return reply.NewIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	tx.mu.Lock()
	defer tx.mu.Unlock()
	err := tx.rollbackWithLock()
	if err != nil {
		return &reply.NormalErrReply{Status: err.Error()}
	}
	// clean transaction
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.txMutex.Lock()
		cluster.transactions.Remove(tx.id)
		cluster.txMutex.Unlock()
	})
	return reply.NewIntReply(1)
}

// execCommit commits local transaction as a worker when receive execCommit command from coordinator
func execCommit(cluster *ClusterDatabase, _ resp.Connection, cmdLine db.CmdLine) resp.Reply {
	if len(cmdLine) != 2 {
		return reply.NewArgNumErrReply(enum.TCC_COMMIT.String())
	}
	txID := string(cmdLine[1])
	cluster.txMutex.RLock()
	raw, ok := cluster.transactions.Get(txID)
	cluster.txMutex.RUnlock()
	if !ok {
		return reply.NewIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	/*tx.mu.Lock()
	defer tx.mu.Unlock()

	result := cluster.db.ExecWithoutLock(conn, tx.cmdLine)

	if reply.IsErrReply(result) {
		// failed
		err2 := tx.rollbackWithLock()
		return reply.NewErrReply(fmt.Sprintf("occurs when rollback: %v, origin err: %s", err2, result))
	}
	// after committed
	tx.unLockKeys()
	tx.status = committedStatus
	// clean finished transaction
	// do not clean immediately, in case rollback
	timewheel.Delay(waitBeforeCleanTx, "", func() {
		cluster.txMutex.Lock()
		cluster.transactions.Remove(tx.id)
		cluster.txMutex.Unlock()
	})*/
	result, err := tx.commit()
	if err != nil {
		return err
	}
	return result
}

// requestCommit 给事务相关的节点发送commit指令, 返回各节点的回复, 解除相关的读写锁
func requestCommit(
	cluster *ClusterDatabase,
	conn resp.Connection,
	txID int64,
	groupMap map[string][]string) ([]resp.Reply, resp.ErrorReply) {

	var errReply resp.ErrorReply
	txIDStr := strconv.FormatInt(txID, 10)
	respList := make([]resp.Reply, 0, len(groupMap))
	cmd := utils.ToCmdLine(enum.TCC_COMMIT.String(), txIDStr)

	for node := range groupMap {
		if node == cluster.self {
			execCommit(cluster, conn, cmd)
			continue
		}
		r := cluster.relay(node, conn, cmd)
		if reply.IsErrReply(r) {
			errReply = r.(resp.ErrorReply)
			break
		}
		respList = append(respList, r)
	}
	if errReply != nil {
		requestRollback(cluster, conn, txID, groupMap)
		return nil, errReply
	}
	return respList, nil
}

// requestRollback requests all node rollback transaction as coordinator
// groupMap: node -> keys
func requestRollback(cluster *ClusterDatabase, conn resp.Connection, txID int64, groupMap map[string][]string) {
	txIDStr := strconv.FormatInt(txID, 10)
	cmd := utils.ToCmdLine(enum.TCC_ROLLBACK.String(), txIDStr)

	for node := range groupMap {
		if node == cluster.self {
			execRollback(cluster, conn, cmd)
			continue
		}
		cluster.relay(node, conn, cmd)
	}
}
