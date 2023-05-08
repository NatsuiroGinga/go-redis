package database

import (
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
	"strconv"
)

// GET key
var execGet = ExecFunc(func(d *DB, args db.CmdLine) resp.Reply {
	if len(args) != 1 {
		return reply.NewArgNumErrReply(enum.GET.String())
	}

	key := string(args[0])
	entity, ok := d.GetEntity(key)
	if !ok {
		return reply.NewNullBulkReply()
	}
	value, ok := entity.Data.([]byte)
	if !ok {
		return reply.NewWrongTypeErrReply()
	}

	return reply.NewBulkReply(value)
})

// SET key value
var execSet = ExecFunc(func(d *DB, args db.CmdLine) resp.Reply {
	if len(args) != 2 {
		return reply.NewArgNumErrReply(enum.SET.String())
	}

	key := string(args[0])
	value := args[1]
	d.PutEntity(key, db.NewDataEntity(value))
	d.append(utils.ToCmdLine2(enum.SET.String(), args...))

	return reply.NewOKReply()
})

// SETNX key value
var execSetNX = ExecFunc(func(d *DB, args db.CmdLine) resp.Reply {
	if len(args) != 2 {
		return reply.NewArgNumErrReply(enum.SETNX.String())
	}

	key := string(args[0])
	value := args[1]
	n := d.PutIfAbsent(key, db.NewDataEntity(value))
	d.append(utils.ToCmdLine2(enum.SETNX.String(), args...))

	return reply.NewIntReply(int64(n))
})

var execGetSet = ExecFunc(func(d *DB, args db.CmdLine) resp.Reply {
	if len(args) != 2 {
		return reply.NewArgNumErrReply(enum.GETSET.String())
	}
	key := string(args[0])
	value := args[1]

	entity, ok := d.GetEntity(key)
	d.PutEntity(key, db.NewDataEntity(value))
	d.append(utils.ToCmdLine2(enum.GETSET.String(), args...))

	if !ok {
		return reply.NewNullBulkReply()
	}

	oldValue, ok := entity.Data.([]byte)
	if !ok {
		return reply.NewWrongTypeErrReply()
	}

	return reply.NewBulkReply(oldValue)
})

// STRLEN key
var execStrLen = ExecFunc(func(d *DB, args db.CmdLine) resp.Reply {
	key := string(args[0])
	entity, ok := d.GetEntity(key)
	if !ok {
		return reply.NewNullBulkReply()
	}
	value, ok := entity.Data.([]byte)

	if ok {
		return reply.NewIntReply(int64(len(value)))
	}

	return reply.NewWrongTypeErrReply()
})

// execIncr INCR key
var execIncr = ExecFunc(func(d *DB, args db.CmdLine) resp.Reply {
	key := string(args[0])
	entity, ok := d.GetEntity(key)
	if !ok {
		d.PutEntity(key, db.NewDataEntity(utils.String2Bytes("1")))
		return reply.NewIntReply(1)
	}
	value, ok := entity.Data.([]byte)
	if !ok {
		return reply.NewWrongTypeErrReply()
	}

	num, err := strconv.Atoi(utils.Bytes2String(value))
	if err != nil {
		return reply.NewIntErrReply()
	}

	num++
	entity.Data = utils.String2Bytes(strconv.Itoa(num))
	d.PutEntity(key, entity)

	return reply.NewIntReply(int64(num))
})

func init() {
	RegisterCommand(enum.GET, execGet)
	RegisterCommand(enum.SET, execSet)
	RegisterCommand(enum.SETNX, execSetNX)
	RegisterCommand(enum.GETSET, execGetSet)
	RegisterCommand(enum.STRLEN, execStrLen)
	RegisterCommand(enum.INCR, execIncr)
}
