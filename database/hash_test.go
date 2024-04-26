package database

import (
	"testing"

	"go-redis/enum"
	"go-redis/lib/utils"
)

func TestExecHVals(t *testing.T) {
	d := newDB(0)
	d.execWithLock(utils.ToCmdLine(enum.HSET.String(), "myhash", "field1", "foo"))
	d.execWithLock(utils.ToCmdLine(enum.HSET.String(), "myhash", "field2", "bar"))
	reply := d.execWithLock(utils.ToCmdLine(enum.HVALS.String(), "myhash"))
	t.Log(string(reply.Bytes()))
}

func TestExecHGetAll(t *testing.T) {
	d := newDB(0)
	d.execWithLock(utils.ToCmdLine(enum.HSET.String(), "myhash", "field1", "Hello"))
	d.execWithLock(utils.ToCmdLine(enum.HSET.String(), "myhash", "field2", "World"))
	reply := d.execWithLock(utils.ToCmdLine(enum.HGETALL.String(), "myhash"))
	t.Log(string(reply.Bytes()))
}

func TestExecHMGet(t *testing.T) {
	d := newDB(0)
	d.execWithLock(utils.ToCmdLine(enum.HSET.String(), "myhash", "field1", "foo"))
	d.execWithLock(utils.ToCmdLine(enum.HSET.String(), "myhash", "field2", "bar"))
	reply := d.execWithLock(utils.ToCmdLine(enum.HMGET.String(), "myhash", "field1", "field2"))
	t.Log(string(reply.Bytes()))
}
