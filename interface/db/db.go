package db

import (
	"go-redis/interface/resp"
	"io"
)

// CmdLine 表示一行命令
type CmdLine = [][]byte

type Database interface {
	Exec(client resp.Connection, args CmdLine) resp.Reply
	io.Closer
	AfterClientClose(client resp.Connection)
}

type DataEntity struct {
	Data any
}

func NewDataEntity(data any) *DataEntity {
	return &DataEntity{Data: data}
}
