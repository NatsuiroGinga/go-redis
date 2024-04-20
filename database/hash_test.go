package database

import (
	"testing"

	"go-redis/enum"
	"go-redis/lib/utils"
)

func TestExecHVals(t *testing.T) {
	d := newDB(0)
	d.exec(utils.ToCmdLine(enum.HSET.String(), "myhash", "field1", "foo"))
	d.exec(utils.ToCmdLine(enum.HSET.String(), "myhash", "field2", "bar"))
	reply := d.exec(utils.ToCmdLine(enum.HVALS.String(), "myhash"))
	t.Log(string(reply.Bytes()))
}

func TestExecHGetAll(t *testing.T) {
	d := newDB(0)
	d.exec(utils.ToCmdLine(enum.HSET.String(), "myhash", "field1", "Hello"))
	d.exec(utils.ToCmdLine(enum.HSET.String(), "myhash", "field2", "World"))
	reply := d.exec(utils.ToCmdLine(enum.HGETALL.String(), "myhash"))
	t.Log(string(reply.Bytes()))
}

func TestExecHMGet(t *testing.T) {
	d := newDB(0)
	d.exec(utils.ToCmdLine(enum.HSET.String(), "myhash", "field1", "foo"))
	d.exec(utils.ToCmdLine(enum.HSET.String(), "myhash", "field2", "bar"))
	reply := d.exec(utils.ToCmdLine(enum.HMGET.String(), "myhash", "field1", "field2"))
	t.Log(string(reply.Bytes()))
}
