package database

import (
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func execPing(_ *DB, _ db.Params) resp.Reply {
	return reply.NewPongReply()
}

func init() {
	registerCommand(enum.PING, noPrepare, execPing)
}
