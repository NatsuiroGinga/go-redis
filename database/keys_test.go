package database

import (
	"strconv"
	"testing"
	"time"

	"go-redis/lib/logger"
	"go-redis/lib/utils"
)

func TestExecSetAndExecGet(t *testing.T) {
	d := newDB(0)
	execSet(d, utils.ToCmdLine("name", "jack"))
	reply := execGet(d, utils.ToCmdLine("name"))
	logger.Info("reply:", string(reply.Bytes()))
}

func TestExecExpireAndExecExpireTime(t *testing.T) {
	d := newDB(0)
	execSet(d, utils.ToCmdLine("name", "jack"))
	execExpire(d, utils.ToCmdLine("name", "10"))
	reply := execExpireTime(d, utils.ToCmdLine("name"))
	logger.Info("reply:", string(reply.Bytes()))
}

func TestExecExpireAtAndExecTTL(t *testing.T) {
	d := newDB(0)
	execSet(d, utils.ToCmdLine("name", "jack"))
	expireAt := time.Now().Add(11 * time.Second).Unix()
	execExpireAt(d, utils.ToCmdLine("name", strconv.FormatInt(expireAt, 10)))
	reply := execTTL(d, utils.ToCmdLine("name"))
	logger.Info("reply:", string(reply.Bytes()))
}

func TestExecKeys(t *testing.T) {
	d := newDB(0)
	execSet(d, utils.ToCmdLine("name", "jack"))
	execSet(d, utils.ToCmdLine("age", "12"))
	execSet(d, utils.ToCmdLine("country", "china"))
	reply := execKeys(d, utils.ToCmdLine("*"))
	logger.Info("reply:", string(reply.Bytes()))
}

func TestDemo(t *testing.T) {
	seconds := time.Now().Unix()
	t.Log(seconds)
}
