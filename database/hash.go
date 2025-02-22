package database

import (
	"go-redis/datastruct/dict"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

func (d *DB) getDict(key string) (dict.Dict, resp.ErrorReply) {
	entity, exist := d.getEntity(key)
	if !exist {
		return nil, nil
	}
	dt, ok := entity.Data.(dict.Dict)
	if !ok {
		return nil, reply.NewWrongTypeErrReply()
	}
	return dt, nil
}

func (d *DB) getOrCreateDict(key string) (hashTable dict.Dict, created bool, errorReply resp.ErrorReply) {
	hashTable, errorReply = d.getDict(key)
	if errorReply != nil {
		return nil, false, errorReply
	}
	created = false
	if hashTable == nil {
		hashTable = dict.NewNormalDict()
		d.putEntity(key, db.NewDataEntity(hashTable))
		created = true
	}
	return hashTable, created, nil
}

// execHSet 命令用于为哈希表中的字段赋值 。
//
// 如果哈希表不存在，一个新的哈希表被创建并进行 HSET 操作。
//
// 如果字段已经存在于哈希表中，旧值将被覆盖。
//
// # HSET KEY_NAME FIELD VALUE
//
// 返回: 如果字段是哈希表中的一个新建字段，并且值设置成功，返回 1 。 如果哈希表中域字段已经存在且旧值已被新值覆盖，返回 0
func execHSet(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	field := utils.Bytes2String(args[1])
	value := parseBytes(args[2])

	hashTable, _, errReply := d.getOrCreateDict(key)
	if errReply != nil {
		return errReply
	}

	result := hashTable.Set(field, value)
	d.append(utils.ToCmdLine2(enum.HSET.String(), args...))
	return reply.NewIntReply(int64(result))
}

// execHSetNX 命令用于为哈希表中不存在的的字段赋值
//
// 如果哈希表不存在，一个新的哈希表被创建并进行 HSET 操作
//
// 如果字段已经存在于哈希表中，操作无效
//
// 如果 key 不存在，一个新哈希表被创建并执行 HSETNX 命令
//
// # HSETNX KEY_NAME FIELD VALUE
//
// 返回: 设置成功，返回 1. 如果给定字段已经存在且没有操作被执行，返回 0
func execHSetNX(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	field := utils.Bytes2String(args[1])
	value := parseBytes(args[2])

	hashTable, _, errReply := d.getOrCreateDict(key)
	if errReply != nil {
		return errReply
	}

	result := hashTable.PutIfAbsent(field, value)
	if result > 0 {
		d.append(utils.ToCmdLine2(enum.HSETNX.String(), args...))

	}
	return reply.NewIntReply(int64(result))
}

// execHGet 命令用于返回哈希表中指定字段的值。
//
// # HGET KEY_NAME FIELD_NAME
//
// 返回: 返回给定字段的值。如果给定的字段或 key 不存在时，返回 nil
func execHGet(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	field := utils.Bytes2String(args[1])

	hashTable, errReply := d.getDict(key)
	if errReply != nil {
		return errReply
	}
	if hashTable == nil {
		return reply.NewNullBulkReply()
	}

	raw, exists := hashTable.Get(field)
	if !exists {
		return reply.NewNullBulkReply()
	}

	return reply.NewBulkReply(parseAny(raw))
}

// execHExists 命令用于查看哈希表的指定字段是否存在。
//
// # HEXISTS KEY_NAME FIELD_NAME
//
// 返回: 如果哈希表含有给定字段，返回 1. 如果哈希表不含有给定字段，或 key 不存在，返回 0
func execHExists(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	field := utils.Bytes2String(args[1])

	hashTable, errReply := d.getDict(key)
	if errReply != nil {
		return errReply
	}
	if hashTable == nil {
		return reply.NewIntReply(0)
	}

	_, exists := hashTable.Get(field)
	if exists {
		return reply.NewIntReply(1)
	}
	return reply.NewIntReply(0)
}

// execHDel 命令用于删除哈希表 key 中的一个或多个指定字段，不存在的字段将被忽略。
//
// # HDEL KEY_NAME FIELD1.. FIELDN
//
// 返回: 被成功删除字段的数量，不包括被忽略的字段。
func execHDel(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])

	hashTable, errReply := d.getDict(key)
	if errReply != nil {
		return errReply
	}
	if hashTable == nil {
		return reply.NewIntReply(0)
	}

	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = utils.Bytes2String(v)
	}

	deleted := 0
	for _, field := range fields {
		result := hashTable.Remove(field)
		deleted += result
	}
	if hashTable.Len() == 0 {
		d.Remove(key)
	}
	if deleted > 0 {
		d.append(utils.ToCmdLine2(enum.HDEL.String(), args...))
	}

	return reply.NewIntReply(int64(deleted))
}

// execHLen 命令用于获取哈希表中字段的数量。
//
// # HLEN KEY_NAME
//
// 返回: 哈希表中字段的数量. 当 key 不存在时，返回 0
func execHLen(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])

	hashTable, errReply := d.getDict(key)
	if errReply != nil {
		return errReply
	}
	if hashTable == nil {
		return reply.NewIntReply(0)
	}
	return reply.NewIntReply(int64(hashTable.Len()))
}

// execHMSet 命令用于同时将多个 field-value (字段-值)对设置到哈希表中。
//
// 此命令会覆盖哈希表中已存在的字段。
//
// 如果哈希表不存在，会创建一个空哈希表，并执行 HMSET 操作。
//
// # HMSET KEY_NAME FIELD1 VALUE1 ...FIELDN VALUEN
//
// 返回: 如果命令执行成功，返回 OK
func execHMSet(d *DB, args db.Params) resp.Reply {
	if len(args)%2 != 1 {
		return reply.NewSyntaxErrReply()
	}
	key := utils.Bytes2String(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		fields[i] = utils.Bytes2String(args[2*i+1])
		values[i] = args[2*i+2]
	}

	hashTable, _, errReply := d.getOrCreateDict(key)
	if errReply != nil {
		return errReply
	}

	for i, field := range fields {
		value := parseBytes(values[i])
		hashTable.Set(field, value)
	}
	d.append(utils.ToCmdLine2(enum.HMSET.String(), args...))
	return reply.NewOKReply()
}

// execHVals 命令返回哈希表所有的值。
//
// # HVALS key
//
// 返回: 一个包含哈希表中所有值的列表。 当 key 不存在时，返回一个空表。
func execHVals(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	hashTable, errorReply := d.getDict(key)
	if errorReply != nil {
		return errorReply
	}
	if hashTable == nil {
		return reply.NewEmptyMultiBulkReply()
	}

	values := make([][]byte, 0, hashTable.Len())
	hashTable.ForEach(func(field string, value any) bool {
		values = append(values, parseAny(value))
		return true
	})

	return reply.NewMultiBulkReply(values)
}

// execHKeys 命令用于获取哈希表中的所有域（field）
//
// # HKEYS key
//
// 返回: 包含哈希表中所有域（field）列表。 当 key 不存在时，返回一个空列表。
func execHKeys(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])

	hashTable, errReply := d.getDict(key)
	if errReply != nil {
		return errReply
	}
	if hashTable == nil {
		return reply.NewEmptyMultiBulkReply()
	}

	fields := make([][]byte, 0, hashTable.Len())
	hashTable.ForEach(func(field string, val interface{}) bool {
		fields = append(fields, utils.String2Bytes(field))
		return true
	})
	return reply.NewMultiBulkReply(fields)
}

// execHGetAll 命令用于返回哈希表中，所有的字段和值。
//
// 在返回值里，紧跟每个字段名(field name)之后是字段的值(value)，所以返回值的长度是哈希表大小的两倍。
//
// # HGETALL KEY_NAME
//
// 返回: 以列表形式返回哈希表的字段及字段值。 若 key 不存在，返回空列表。
func execHGetAll(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	hashTable, errReply := d.getDict(key)
	if errReply != nil {
		return errReply
	}
	if hashTable == nil {
		return reply.NewEmptyMultiBulkReply()
	}

	entries := make([][]byte, 0, hashTable.Len()*2)
	hashTable.ForEach(func(field string, val any) bool {
		entries = append(entries, utils.String2Bytes(field), parseAny(val))
		return true
	})

	return reply.NewMultiBulkReply(entries)
}

// execHMGet 命令用于返回哈希表中，一个或多个给定字段的值。
//
// 如果指定的字段不存在于哈希表，那么返回一个 nil 值。
//
// # HMGET KEY_NAME FIELD1...FIELDN
//
// 返回: 一个包含多个给定字段关联值的表，表值的排列顺序和指定字段的请求顺序一样。
func execHMGet(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])

	hashTable, errReply := d.getDict(key)
	if errReply != nil {
		return errReply
	}
	if hashTable == nil {
		return reply.NewEmptyMultiBulkReply()
	}

	values := make([][]byte, 0, len(args)-1)
	for _, fieldBytes := range args[1:] {
		field := utils.Bytes2String(fieldBytes)
		value, exist := hashTable.Get(field)
		if !exist {
			values = append(values, nil)
		} else {
			values = append(values, parseAny(value))
		}
	}

	return reply.NewMultiBulkReply(values)
}

func init() {
	registerCommand(enum.HVALS, readFirstKey, execHVals, nil)
	registerCommand(enum.HSET, writeFirstKey, execHSet, undoHSet)
	registerCommand(enum.HSETNX, writeFirstKey, execHSetNX, undoHSet)
	registerCommand(enum.HKEYS, readFirstKey, execHKeys, nil)
	registerCommand(enum.HGET, readFirstKey, execHGet, nil)
	registerCommand(enum.HEXISTS, readFirstKey, execHExists, nil)
	registerCommand(enum.HDEL, writeFirstKey, execHDel, undoHDel)
	registerCommand(enum.HLEN, readFirstKey, execHLen, nil)
	registerCommand(enum.HGETALL, readFirstKey, execHGetAll, nil)
	registerCommand(enum.HMSET, writeFirstKey, execHMSet, undoHMSet)
	registerCommand(enum.HMGET, readFirstKey, execHMGet, nil)
}

func undoHMSet(d *DB, args db.Params) []db.CmdLine {
	key := utils.Bytes2String(args[0])
	size := (len(args) - 1) / 2
	fields := make([]string, size)
	for i := 0; i < size; i++ {
		fields[i] = utils.Bytes2String(args[2*i+1])
	}
	return rollbackHashFields(d, key, fields...)
}

func undoHDel(d *DB, args db.Params) []db.CmdLine {
	key := utils.Bytes2String(args[0])
	params := args[1:]
	fields := make([]string, 0, len(params))
	for _, param := range params {
		fields = append(fields, utils.Bytes2String(param))
	}
	return rollbackHashFields(d, key, fields...)
}

func undoHSet(d *DB, args db.Params) []db.CmdLine {
	key := utils.Bytes2String(args[0])
	field := utils.Bytes2String(args[1])
	return rollbackHashFields(d, key, field)
}
