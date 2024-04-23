package database

import (
	"strings"

	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// GetUndoLogs return rollback commands
func (d *DB) GetUndoLogs(args db.CmdLine) []db.CmdLine {
	cmdName := strings.ToLower(utils.Bytes2String(args[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return nil
	}
	if cmd.undo == nil {
		return nil
	}
	return cmd.undo(d, args[1:])
}

// EnqueueCmd 把命令加入事务的命令队列
func EnqueueCmd(conn resp.Connection, cmdLine db.CmdLine) resp.Reply {
	cmdName := strings.ToLower(utils.Bytes2String(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		err := reply.NewUnknownCommandErrReply(cmdName)
		conn.AddTxError(err)
		return err
	}
	if cmd.prepare == nil {
		err := &reply.NormalErrReply{Status: "command '" + cmdName + "' cannot be used in MULTI"}
		conn.AddTxError(err)
		return err
	}
	if !validateArity(cmd.arity, cmdLine) {
		err := reply.NewArgNumErrReply(cmdName)
		conn.AddTxError(err)
		return err
	}
	conn.EnqueueCmd(cmdLine)
	return reply.NewQueuedReply()
}
