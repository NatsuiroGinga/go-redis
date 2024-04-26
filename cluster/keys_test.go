package cluster_database

import (
	"testing"

	"go-redis/lib/asserts"
	"go-redis/lib/utils"
	"go-redis/resp/connection"
)

func TestExists(t *testing.T) {
	conn := new(connection.FakeConn)

	// cross node rename
	testNodeA.Exec(conn, utils.ToCmdLine("FlushALL"))
	key := "RhhpMJExRb"
	value := utils.RandString(10)
	newKey := "JcRYPdXAV1"
	testNodeA.Exec(conn, utils.ToCmdLine("SET", key, value))
	result := testNodeA.Exec(conn, utils.ToCmdLine("SET", newKey, value))
	asserts.AssertStatusReply(t, result, "OK")
	result = testNodeA.Exec(conn, utils.ToCmdLine("EXISTS", key, newKey))
	asserts.AssertIntReply(t, result, 2)
	// asserts.AssertIntReplyGreaterThan(t, result, 0)
}

func TestKeys(t *testing.T) {
	conn := new(connection.FakeConn)
	testNodeA.Exec(conn, utils.ToCmdLine("FlushALL"))
	key := "RhhpMJExRb"
	value := utils.RandString(10)
	newKey := "JcRYPdXAV1"
	testNodeA.Exec(conn, utils.ToCmdLine("SET", key, value))
	result := testNodeA.Exec(conn, utils.ToCmdLine("SET", newKey, value))
	asserts.AssertStatusReply(t, result, "OK")
	result = testNodeA.Exec(conn, utils.ToCmdLine("KEYS", "*"))
	asserts.AssertIntReply(t, result, 2)
	asserts.AssertMultiBulkReply(t, result, []string{key, newKey})
}
