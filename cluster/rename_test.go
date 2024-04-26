package cluster_database

import (
	"testing"

	"go-redis/lib/asserts"
	"go-redis/lib/utils"
	"go-redis/resp/connection"
)

func TestRename(t *testing.T) {
	conn := new(connection.FakeConn)

	// cross node rename
	for i := 0; i < 10; i++ {
		testNodeA.Exec(conn, utils.ToCmdLine("FlushALL"))
		key := "RhhpMJExRb"
		value := utils.RandString(10)
		newKey := "JcRYPdXAV1"
		testNodeA.Exec(conn, utils.ToCmdLine("SET", key, value))
		testNodeA.Exec(conn, utils.ToCmdLine("EXPIRE", key, "100000"))
		result := testNodeA.Exec(conn, utils.ToCmdLine("RENAME", key, newKey))
		asserts.AssertStatusReply(t, result, "OK")
		result = testNodeA.Exec(conn, utils.ToCmdLine("EXISTS", key))
		asserts.AssertIntReply(t, result, 0)
		result = testNodeA.Exec(conn, utils.ToCmdLine("EXISTS", newKey))
		asserts.AssertIntReply(t, result, 1)
		result = testNodeA.Exec(conn, utils.ToCmdLine("TTL", newKey))
		asserts.AssertIntReplyGreaterThan(t, result, 0)
	}
}
