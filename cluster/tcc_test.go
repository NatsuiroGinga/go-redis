package cluster_database

import (
	"math/rand"
	"strconv"
	"testing"

	"go-redis/config"
	"go-redis/lib/asserts"
	"go-redis/lib/utils"
	"go-redis/resp/connection"
)

var testNodeA = newTestNode()

func newTestNode() *ClusterDatabase {
	config.SetupConfig("../redis.conf")
	node := NewClusterDatabase()
	return node
}

func TestRollback(t *testing.T) {
	// rollback uncommitted transaction
	conn := new(connection.FakeConn)
	// FlushAll(testNodeA, conn, toArgs("FLUSHALL"))
	txID := rand.Int63()
	txIDStr := strconv.FormatInt(txID, 10)
	keys := []string{"a", "{a}1"}
	groupMap := map[string][]string{
		testNodeA.self: keys,
	}
	args := []string{txIDStr, "DEL"}
	args = append(args, keys...)
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", "a", "a"))
	ret := execPrepare(testNodeA, conn, makeArgs("Prepare", args...))
	asserts.AssertNotError(t, ret)
	requestRollback(testNodeA, conn, txID, groupMap)
	ret = testNodeA.db.Exec(conn, utils.ToCmdLine("GET", "a"))
	asserts.AssertBulkReply(t, ret, "a")

	// rollback committed transaction
	execFlushAll(testNodeA, conn, utils.ToCmdLine2("FLUSHALL"))
	testNodeA.db.Exec(conn, utils.ToCmdLine("SET", "a", "a"))
	txID = rand.Int63()
	txIDStr = strconv.FormatInt(txID, 10)
	args = []string{txIDStr, "DEL"}
	args = append(args, keys...)
	ret = execPrepare(testNodeA, conn, makeArgs("Prepare", args...))
	asserts.AssertNotError(t, ret)
	_, err := requestCommit(testNodeA, conn, txID, groupMap)
	if err != nil {
		t.Errorf("del failed %v", err)
		return
	}
	ret = testNodeA.db.Exec(conn, utils.ToCmdLine("GET", "a")) // call db.Exec to skip key router
	asserts.AssertNullBulk(t, ret)
	requestRollback(testNodeA, conn, txID, groupMap)
	ret = testNodeA.db.Exec(conn, utils.ToCmdLine("GET", "a"))
	asserts.AssertBulkReply(t, ret, "a")
}
