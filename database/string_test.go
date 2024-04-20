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
	execIncr(d, [][]byte{[]byte("age")})
	reply := execGet(d, [][]byte{[]byte("age")})
	logger.Info("reply:", string(reply.Bytes()))
}

func TestGet(t *testing.T) {
	d := newDB(0)
	key := "salary"
	args := slices.Concat(
		utils.ToCmdLine(key, "10086", "jack"),
		utils.ToCmdLine("7500", "peter"),
		utils.ToCmdLine("3500", "joe"),
	)
	reply := execZAdd(d, args)
	t.Log(string(reply.Bytes()))

	reply = execGet(d, utils.ToCmdLine(key))
	t.Log(string(reply.Bytes()))
}
