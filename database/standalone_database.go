package database

import (
	"strconv"
	"strings"
	"time"

	"go-redis/aof"
	"go-redis/config"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// StandaloneDatabase 单机版数据库
type StandaloneDatabase struct {
	dbSet      []*DB        // 数据库集合
	aofHandler *aof.Handler // aof处理器
}

func (database *StandaloneDatabase) ExecWithLock(conn resp.Connection, cmdLine db.CmdLine) resp.Reply {
	// TODO implement me
	panic("implement me")
}

func (database *StandaloneDatabase) ExecMulti(conn resp.Connection, watching map[string]uint32, cmdLines []db.CmdLine) resp.Reply {
	// TODO implement me
	panic("implement me")
}

func (database *StandaloneDatabase) GetUndoLogs(dbIndex int, cmdLine [][]byte) []db.CmdLine {
	// TODO implement me
	panic("implement me")
}

func (database *StandaloneDatabase) ForEach(dbIndex int, cb func(key string, data *db.DataEntity, expiration *time.Time) bool) {
	// TODO implement me
	panic("implement me")
}

func (database *StandaloneDatabase) RWLocks(dbIndex int, writeKeys []string, readKeys []string) {
	// TODO implement me
	panic("implement me")
}

func (database *StandaloneDatabase) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	// TODO implement me
	panic("implement me")
}

func (database *StandaloneDatabase) GetDBSize(dbIndex int) (int, int) {
	// TODO implement me
	panic("implement me")
}

func (database *StandaloneDatabase) GetEntity(dbIndex int, key string) (*db.DataEntity, bool) {
	// TODO implement me
	panic("implement me")
}

func (database *StandaloneDatabase) GetExpiration(dbIndex int, key string) *time.Time {
	// TODO implement me
	panic("implement me")
}

func NewStandaloneDatabase() *StandaloneDatabase {
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}

	dbSet := make([]*DB, config.Properties.Databases)
	for i := range dbSet {
		dbSet[i] = newDB(i)
	}
	d := &StandaloneDatabase{dbSet: dbSet}

	// aof
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewHandler(d)
		if err != nil {
			panic(err)
		}
		d.aofHandler = aofHandler
		// 将aofHandler.Append方法赋值给每个db
		for i := range dbSet {
			j := i                                    // 闭包, 防止循环变量被修改
			dbSet[j].append = func(line db.CmdLine) { // 给每个数据库添加aof落盘函数
				aofHandler.Append(dbSet[j].index, line)
			}
		}
	}

	return d
}

func (database *StandaloneDatabase) Exec(client resp.Connection, args db.CmdLine) resp.Reply {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()
	cmdName := strings.ToUpper(utils.Bytes2String(args[0]))
	// select command
	if cmdName == enum.SELECT.String() {
		if len(args) == 2 {
			return execSelect(client, database, args[1:])
		}
		return reply.NewArgNumErrReply(cmdName)
	}

	dbIndex := client.GetDBIndex()
	d := database.dbSet[dbIndex]
	return d.Exec(client, args)
}

func (database *StandaloneDatabase) Close() error {
	return nil
}

func (database *StandaloneDatabase) AfterClientClose(_ resp.Connection) {
}

func execSelect(conn resp.Connection, database *StandaloneDatabase, args db.CmdLine) resp.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.NewErrReply("invalid DB index")
	}
	if dbIndex >= len(database.dbSet) {
		return reply.NewErrReply("invalid DB index")
	}
	conn.SelectDB(dbIndex)

	return reply.NewOKReply()
}
