package database

import (
	"go-redis/enum"
	"strings"
)

var cmdTable = make(map[string]*command)

type command struct {
	executor ExecFunc // 命令执行函数
	arity    int      // 带命令本身的参数数量
}

func RegisterCommand(cmd *enum.Command, executor ExecFunc) {
	cmdTable[strings.ToLower(cmd.Name())] = &command{executor: executor, arity: cmd.Arity()}
}
