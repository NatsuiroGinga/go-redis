package database

import (
	"slices"
	"testing"

	"go-redis/lib/logger"
	"go-redis/lib/utils"
)

func TestINCR(t *testing.T) {
	d := newDB(0)
	execSet(d, utils.ToCmdLine("age", "12"))
	execDecr(d, utils.ToCmdLine("age"))
	reply := execGet(d, utils.ToCmdLine("age"))
	logger.Info("reply:", string(reply.Bytes()))
}

func TestGet(t *testing.T) {
	d := newDB(0)
	args := slices.Concat(
		utils.ToCmdLine("jack", "10086"),
		utils.ToCmdLine("peter", "7500"),
		utils.ToCmdLine("joe", "3500"),
	)
	reply := execMSet(d, args)
	t.Log(string(reply.Bytes()))

	reply = execGet(d, utils.ToCmdLine("jack"))
	t.Log(string(reply.Bytes()))
}

func TestMGET(t *testing.T) {
	d := newDB(0)
	args := slices.Concat(
		utils.ToCmdLine("name", "jack"),
		utils.ToCmdLine("age", "18"),
	)
	execMSet(d, args)

	args = utils.ToCmdLine("name", "nil")
	r := execMGet(d, args)
	t.Log(string(r.Bytes()))
}
