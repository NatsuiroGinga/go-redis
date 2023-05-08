package database

import (
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

type EchoDatabase struct {
}

func (e *EchoDatabase) Exec(_ resp.Connection, args db.CmdLine) resp.Reply {
	return reply.NewMultiBulkReply(args)
}

func (e *EchoDatabase) Close() error {
	return nil
}

func (e *EchoDatabase) AfterClientClose(_ resp.Connection) {
}

func NewEchoDatabase() *EchoDatabase {
	return &EchoDatabase{}
}
