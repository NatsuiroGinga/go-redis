package database

import (
	"go-redis/datastruct/dict"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
	"strings"
)

// DB is the redis database.
type DB struct {
	index  int
	data   dict.Dict
	append func(db.CmdLine)
}

// Exec executes the command.
func (d *DB) Exec(cmd db.CmdLine) resp.Reply {
	if len(cmd) == 0 {
		return reply.NewNoReply()
	}
	instruction := strings.ToLower(string(cmd[0]))
	com, ok := cmdTable[instruction]
	if !ok {
		return reply.NewUnknownCommandErrReply(instruction)
	}
	if !validateArity(com.arity, cmd) {
		return reply.NewArgNumErrReply(instruction)
	}

	return com.executor(d, cmd[1:])
}

// Removes remove the keys from the database.
//
// It returns the number of keys that were removed.
func (d *DB) Removes(keys ...string) (n int) {
	for _, key := range keys {
		_, ok := d.data.Get(key)
		if ok {
			d.data.Remove(key)
			n++
		}
	}

	return n
}

// GetEntity returns the entity of the key.
//
// It returns nil if the key does not exist.
// It returns false if the key does not exist.
func (d *DB) GetEntity(key string) (entity *db.DataEntity, ok bool) {
	var value any
	value, ok = d.data.Get(key)
	if !ok {
		return nil, false
	}
	entity = value.(*db.DataEntity)
	return entity, true
}

// PutEntity puts the entity into the database.
//
// It returns the number of keys that were put.
func (d *DB) PutEntity(key string, entity *db.DataEntity) (n int) {
	return d.data.Set(key, entity)
}

// PutIfExists puts the entity into the database if the key exists.
//
// It returns the number of keys that were put.
func (d *DB) PutIfExists(key string, entity *db.DataEntity) (n int) {
	return d.data.PutIfExist(key, entity)
}

// PutIfAbsent puts the entity into the database if the key does not exist.
//
// It returns the number of keys that were put.
func (d *DB) PutIfAbsent(key string, entity *db.DataEntity) (n int) {
	return d.data.PutIfAbsent(key, entity)
}

func validateArity(arity int, cmd db.CmdLine) bool {
	return len(cmd) == arity
}

func (d *DB) Flush() {
	d.data.Clear()
}

// ExecFunc is the function type for all commands.
type ExecFunc func(db *DB, args db.CmdLine) resp.Reply

// NewDB creates a new database with the given index.
func NewDB(index int) *DB {
	return &DB{index: index, data: dict.NewSyncDict(), append: func(db.CmdLine) {}}
}
