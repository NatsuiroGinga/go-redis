package database

import (
	"slices"
	"testing"

	"go-redis/lib/utils"
	reply2 "go-redis/resp/reply"
)

func TestExecZRevRangeByScore(t *testing.T) {
	d := newDB(0)
	key := "salary"
	args := slices.Concat(
		utils.ToCmdLine(key, "10086", "jack"),
		utils.ToCmdLine("7500", "peter"),
		utils.ToCmdLine("3500", "joe"),
	)
	reply := execZAdd(d, args)
	if reply2.IsErrReply(reply) {
		t.Error(reply)
	}

	reply = execZRevRangeByScore(d, utils.ToCmdLine(key, "+inf", "-inf", "withscores"))
	if reply2.IsErrReply(reply) {
		t.Error(reply)
	}
	t.Logf("reply: %s\n", reply.Bytes())

	reply = execZRevRangeByScore(d, utils.ToCmdLine(key, "10000", "2000", "withscores"))
	if reply2.IsErrReply(reply) {
		t.Error(reply)
	}
	t.Logf("reply: %s\n", reply.Bytes())
}
