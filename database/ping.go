package database

import (
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

func execPing(_ *DB, args db.CmdLine) resp.Reply {
	if len(args) != 0 {
		return reply.NewArgNumErrReply(enum.PING.String())
	}

	return reply.NewPongReply()
}

func init() {
	RegisterCommand(enum.PING, execPing)
}
