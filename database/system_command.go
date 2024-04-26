package database

import (
	"go-redis/config"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// Auth 验证密码
func Auth(conn resp.Connection, args db.Params) resp.Reply {
	if len(args) != enum.SYS_AUTH.ParamCount() {
		return reply.NewArgNumErrReply(enum.SYS_AUTH.String())
	}
	if config.Properties.RequirePass == "" {
		return reply.NewErrReply("Client sent AUTH, but no password is set")
	}
	pwd := utils.Bytes2String(args[0])
	conn.SetPassword(pwd)
	if config.Properties.RequirePass != pwd {
		return reply.NewErrReply("invalid password")
	}
	return reply.NewOKReply()
}

// IsAuthenticated 判断是否验证密码成功, 如果没有配置密码或者密码匹配返回true, 否则返回false
func IsAuthenticated(conn resp.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return conn.GetPassword() == config.Properties.RequirePass
}
