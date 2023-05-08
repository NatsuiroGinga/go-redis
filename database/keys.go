package database

import (
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/lib/wildcard"
	"go-redis/resp/reply"
)

// execDel deletes the keys.
func execDel(d *DB, args db.CmdLine) resp.Reply {
	if len(args) == 0 {
		return reply.NewArgNumErrReply(enum.DEL.String())
	}

	keys := make([]string, 0, len(args))
	for _, arg := range args {
		keys = append(keys, string(arg))
	}
	n := d.Removes(keys...)

	if n > 0 {
		d.append(utils.ToCmdLine2(enum.DEL.String(), args...))
	}

	return reply.NewIntReply(int64(n))
}

// execExists returns the number of keys existing.
func execExists(d *DB, args db.CmdLine) resp.Reply {
	if len(args) == 0 {
		return reply.NewArgNumErrReply(enum.EXISTS.String())
	}

	n := int64(0)

	for _, arg := range args {
		key := string(arg)
		_, ok := d.GetEntity(key)
		if ok {
			n++
		}
	}

	return reply.NewIntReply(n)
}

// execFlushDB deletes all the keys.
func execFlushDB(d *DB, args db.CmdLine) resp.Reply {
	if len(args) != 0 {
		return reply.NewArgNumErrReply(enum.FLUSHDB.String())
	}
	d.Flush()
	d.append(utils.ToCmdLine(enum.FLUSHDB.String()))

	return reply.NewOKReply()
}

// execType returns the type of the key.
func execType(d *DB, args db.CmdLine) resp.Reply {
	if len(args) != 1 {
		return reply.NewArgNumErrReply(enum.TYPE.String())
	}

	key := string(args[0])
	entity, ok := d.GetEntity(key)
	if !ok {
		return reply.NewErrReply("none")
	}

	// TODO 实现除了string之外的数据结构
	switch entity.Data.(type) {
	case []byte: // string
		return reply.NewStatusReply(enum.String.String())
	}

	return reply.NewUnknownErrReply()
}

func execRename(d *DB, args db.CmdLine) resp.Reply {
	if len(args) != 2 {
		return reply.NewArgNumErrReply(enum.RENAME.String())
	}

	src := string(args[0])
	dst := string(args[1])
	entity, ok := d.GetEntity(src)
	if !ok {
		return reply.NewErrReply("no such key")
	}
	d.PutEntity(dst, entity)
	d.Removes(src)

	d.append(utils.ToCmdLine2(enum.RENAME.String(), args...))

	return reply.NewOKReply()
}

// execRenameNX rename key1 to key2 if key2 doesn't exist
//
// return 1 if key2 exists, else return 0
func execRenameNX(d *DB, args db.CmdLine) resp.Reply {
	if len(args) != 2 {
		return reply.NewArgNumErrReply(enum.RENAMENX.String())
	}

	src := string(args[0])
	dst := string(args[1])
	entity, ok := d.GetEntity(dst)
	if ok {
		return reply.NewIntReply(0)
	}
	d.PutEntity(dst, entity)
	d.append(utils.ToCmdLine2(enum.RENAMENX.String(), args...))

	return reply.NewIntReply(int64(d.Removes(src)))
}

// execKeys returns all the keys matching the pattern.
func execKeys(d *DB, args db.CmdLine) resp.Reply {
	if len(args) != 1 {
		return reply.NewArgNumErrReply(enum.KEYS.String())
	}

	key := string(args[0])
	pattern := wildcard.CompilePattern(key)
	result := make([][]byte, 0)
	d.data.ForEach(func(key string, value any) {
		if pattern.IsMatch(key) {
			result = append(result, utils.String2Bytes(key))
		}
	})

	return reply.NewMultiBulkReply(result)
}

func init() {
	RegisterCommand(enum.DEL, execDel)
	RegisterCommand(enum.EXISTS, execExists)
	RegisterCommand(enum.FLUSHDB, execFlushDB)
	RegisterCommand(enum.TYPE, execType)
	RegisterCommand(enum.RENAME, execRename)
	RegisterCommand(enum.RENAMENX, execRenameNX)
	RegisterCommand(enum.KEYS, execKeys)
}
