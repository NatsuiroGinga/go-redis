package db

import (
	"io"
	"time"

	"go-redis/interface/resp"
)

// CmdLine 表示一行命令, 包括命令名和参数
type CmdLine = [][]byte

// Params 不包括命令名的参数
type Params = [][]byte

type Database interface {
	// Exec 不加锁的执行命令
	Exec(client resp.Connection, args CmdLine) resp.Reply
	io.Closer
	AfterClientClose(client resp.Connection)
}

type DataEntity struct {
	Data any
}

func NewDataEntity(data any) *DataEntity {
	return &DataEntity{
		Data: data,
	}
}

// DBEngine is the embedding storage engine exposing more methods for complex application
type DBEngine interface {
	Database
	ExecWithoutLock(conn resp.Connection, cmdLine CmdLine) resp.Reply
	ExecMulti(conn resp.Connection, cmdLines []CmdLine) resp.Reply
	GetUndoLogs(dbIndex int, cmdLine [][]byte) []CmdLine
	ForEach(dbIndex int, cb func(key string, data *DataEntity, expiration *time.Time) bool)
	RWLocks(dbIndex int, writeKeys []string, readKeys []string)
	RWUnLocks(dbIndex int, writeKeys []string, readKeys []string)
	GetDBSize(dbIndex int) (int, int)
	GetEntity(dbIndex int, key string) (*DataEntity, bool)
	GetExpiration(dbIndex int, key string) *time.Time
}
