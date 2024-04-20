package db

import (
	"io"

	"go-redis/interface/resp"
)

// CmdLine 表示一行命令, 包括命令名和参数
type CmdLine [][]byte

// Params 不包括命令名的参数
type Params [][]byte

type Database interface {
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
