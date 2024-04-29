package database

import (
	str "go-redis/datastruct/string"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

func (d *DB) getString(key string) (*str.String, resp.ErrorReply) {
	entity, ok := d.getEntity(key)
	if !ok {
		return nil, nil
	}
	l, ok := entity.Data.(*str.String)
	if !ok {
		return nil, reply.NewWrongTypeErrReply()
	}
	return l, nil
}

// execGet 获取key对应的value
//
// 格式: get key, 例如: get jack
//
// 返回key对应的value
func execGet(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	s, errorReply := d.getString(key)
	if errorReply != nil {
		return errorReply
	}
	if s == nil {
		return reply.NewNullBulkReply()
	}
	/*entity, ok := d.getEntity(key)
	if !ok {
		return reply.NewNullBulkReply()
	}
	value, ok := entity.Data.([]byte)
	if !ok {
		return reply.NewWrongTypeErrReply()
	}*/

	return reply.NewBulkReply(s.Bytes())
}

// execSet 设置key-value
//
// 格式: set key value, 例如: set name jack
//
// 成功返回OK
func execSet(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	value := str.NewString(args[1])
	d.putEntity(key, db.NewDataEntity(value))
	d.append(utils.ToCmdLine2(enum.SET.String(), args...))

	return reply.NewOKReply()
}

// execSetNX
func execSetNX(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	value := str.NewString(args[1])
	n := d.putIfAbsent(key, db.NewDataEntity(value))
	d.append(utils.ToCmdLine2(enum.SETNX.String(), args...))

	return reply.NewIntReply(int64(n))
}

// execGetSEt 命令用于设置指定 key 的值，并返回 key 的旧值。
//
// 格式: GETSET KEY_NAME VALUE
//
// 返回给定 key 的旧值。 当 key 没有旧值时，即 key 不存在时，返回 nil
//
// 当 key 存在但不是字符串类型时，返回一个错误。
func execGetSet(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	value := str.NewString(args[1])

	entity, ok := d.getEntity(key)
	d.putEntity(key, db.NewDataEntity(value))
	d.append(utils.ToCmdLine2(enum.GETSET.String(), args...))

	if !ok {
		return reply.NewNullBulkReply()
	}

	oldValue, ok := entity.Data.(*str.String)
	if !ok {
		return reply.NewWrongTypeErrReply()
	}

	return reply.NewBulkReply(oldValue.Bytes())
}

// execStrLen 命令用于获取指定 key 所储存的字符串值的长度。当 key 储存的不是字符串值时，返回一个错误。
//
// 返回字符串值的长度。 当 key 不存在时，返回 0。
func execStrLen(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	entity, ok := d.getEntity(key)
	if !ok {
		return reply.NewNullBulkReply()
	}
	value, ok := entity.Data.(*str.String)

	if ok {
		return reply.NewIntReply(int64(value.Len()))
	}

	return reply.NewWrongTypeErrReply()
}

// execIncr 命令将 key 中储存的数字值增一。如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCR 操作。
//
// 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
//
// 本操作的值限制在 64 位(bit)有符号数字表示之内。
//
// 格式: INCR KEY_NAME
//
// 返回: 执行 INCR 命令之后 key 的值。
func execIncr(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	entity, ok := d.getEntity(key)
	if !ok {
		d.putEntity(key, db.NewDataEntity(str.NewString(1)))
		d.append(utils.ToCmdLine2(enum.INCR.String(), args...))
		return reply.NewIntReply(1)
	}
	value, ok := entity.Data.(*str.String)
	if !ok {
		return reply.NewWrongTypeErrReply()
	}

	// num, err := strconv.Atoi(utils.Bytes2String(value))
	if !value.CanInt() {
		return reply.NewIntErrReply()
	}

	// num++
	// entity.Data = utils.String2Bytes(strconv.Itoa(num))
	value.SetInt(value.Int() + 1)
	entity.Data = value
	d.putEntity(key, entity)

	d.append(utils.ToCmdLine2(enum.INCR.String(), args...))

	return reply.NewIntReply(value.Int())
}

// Redis Decr 命令将 key 中储存的数字值减一。
//
// 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 DECR 操作。
//
// 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
//
// 本操作的值限制在 64 位(bit)有符号数字表示之内。
//
// 格式: DECR KEY_NAME
//
// 返回: 执行命令之后 key 的值。
func execDecr(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	entity, ok := d.getEntity(key)
	if !ok {
		d.putEntity(key, db.NewDataEntity(str.NewString(-1)))
		d.append(utils.ToCmdLine2(enum.DECR.String(), args...))
		return reply.NewIntReply(-1)
	}
	value, ok := entity.Data.(*str.String)
	if !ok {
		return reply.NewWrongTypeErrReply()
	}

	// num, err := strconv.Atoi(utils.Bytes2String(value))
	if !value.CanInt() {
		return reply.NewIntErrReply()
	}

	// num--
	// entity.Data = utils.String2Bytes(strconv.Itoa(num))
	value.SetInt(value.Int() - 1)
	d.putEntity(key, entity)

	d.append(utils.ToCmdLine2(enum.DECR.String(), args...))

	return reply.NewIntReply(value.Int())
}

func execMSet(d *DB, args db.Params) resp.Reply {
	if len(args)%2 != 0 {
		return reply.NewSyntaxErrReply()
	}

	size := len(args) / 2
	keys := make([]string, size)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		keys[i] = utils.Bytes2String(args[2*i])
		values[i] = args[2*i+1]
	}

	for i, key := range keys {
		value := values[i]
		d.putEntity(key, db.NewDataEntity(str.NewString(value)))
	}
	d.append(utils.ToCmdLine2(enum.MSET.String(), args...))
	return reply.NewOKReply()
}

func init() {
	registerCommand(enum.GET, readFirstKey, execGet, nil)
	registerCommand(enum.SET, writeFirstKey, execSet, rollbackFirstKey)
	registerCommand(enum.MSET, prepareMSet, execMSet, undoMSet)
	registerCommand(enum.SETNX, writeFirstKey, execSetNX, rollbackFirstKey)
	registerCommand(enum.GETSET, writeFirstKey, execGetSet, rollbackFirstKey)
	registerCommand(enum.STRLEN, readFirstKey, execStrLen, nil)
	registerCommand(enum.INCR, writeFirstKey, execIncr, rollbackFirstKey)
	registerCommand(enum.DECR, writeFirstKey, execDecr, rollbackFirstKey)
}

func undoMSet(d *DB, args db.Params) []db.CmdLine {
	writeKeys, _ := prepareMSet(args)
	return rollbackKeys(d, writeKeys...)
}
