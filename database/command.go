package database

import (
	"strings"

	"go-redis/enum"
)

var cmdTable = make(map[string]*command)

type command struct {
	executor execFunc // 命令执行函数
	prepare  preFunc  // 执行命令之前的准备函数
	arity    int      // 带命令本身的参数数量
}

func registerCommand(cmd *enum.Command, prepare preFunc, executor execFunc) {
	cmdTable[strings.ToLower(cmd.Name())] = &command{
		executor,
		prepare,
		cmd.Arity(),
	}
}
