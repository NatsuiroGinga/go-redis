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

// ExecWithoutLock 不加锁就执行命令, 并发不安全
func (database *StandaloneDatabase) ExecWithoutLock(conn resp.Connection, cmdLine db.CmdLine) resp.Reply {
	d, errReply := database.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return d.exec(cmdLine)
}

func (database *StandaloneDatabase) ExecMulti(conn resp.Connection, cmdLines []db.CmdLine) resp.Reply {
	selectedDB, errReply := database.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return selectedDB.execMulti(conn, cmdLines)
}

func (database *StandaloneDatabase) GetUndoLogs(dbIndex int, cmdLine db.CmdLine) []db.CmdLine {
	return database.mustSelectDB(dbIndex).GetUndoLogs(cmdLine)
}

func (database *StandaloneDatabase) ForEach(dbIndex int, cb func(key string, data *db.DataEntity, expiration *time.Time) bool) {
	database.mustSelectDB(dbIndex).ForEach(cb)
}

func (database *StandaloneDatabase) RWLocks(dbIndex int, writeKeys, readKeys []string) {
	database.mustSelectDB(dbIndex).RWLocks(writeKeys, readKeys)
}

func (database *StandaloneDatabase) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) {
	database.mustSelectDB(dbIndex).RWUnLocks(writeKeys, readKeys)
}

func (database *StandaloneDatabase) GetDBSize(dbIndex int) (dataSize int, ttlSize int) {
	d := database.mustSelectDB(dbIndex)
	return d.data.Len(), d.ttl.Len()
}

func (database *StandaloneDatabase) GetEntity(dbIndex int, key string) (*db.DataEntity, bool) {
	return database.mustSelectDB(dbIndex).getEntity(key)
}

func (database *StandaloneDatabase) GetExpiration(dbIndex int, key string) *time.Time {
	raw, ok := database.mustSelectDB(dbIndex).ttl.Get(key)
	if !ok {
		return nil
	}
	expireTime, _ := raw.(time.Time)
	return &expireTime
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

	// FlushAll
	if cmdName == enum.FLUSHALL.String() {
		return database.flushAll()
	}
	// Select
	if cmdName == enum.SELECT.String() {
		if ValidateArity(enum.SELECT.Arity(), args) {
			return execSelect(client, database, args[1:])
		}
		return reply.NewArgNumErrReply(cmdName)
	}
	// FlushDB
	if cmdName == enum.FLUSHDB.String() && client.InMultiState() {
		return reply.NewErrReply("command 'FlushDB' cannot be used in MULTI")
	}
	// Auth
	if cmdName == enum.SYS_AUTH.String() {
		return Auth(client, args[1:])
	}
	if !IsAuthenticated(client) {
		return &reply.NormalErrReply{Status: "NOAUTH Authentication required"}
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

func (database *StandaloneDatabase) selectDB(dbIndex int) (*DB, resp.ErrorReply) {
	if dbIndex >= len(database.dbSet) || dbIndex < 0 {
		return nil, reply.NewErrReply("DB index is out of range")
	}
	return database.dbSet[dbIndex], nil
}

// mustSelectDB is like selectDB, but panics if an error occurs.
func (database *StandaloneDatabase) mustSelectDB(dbIndex int) *DB {
	selectedDB, err := database.selectDB(dbIndex)
	if err != nil {
		logger.Panic(err)
	}
	return selectedDB
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

// flushAll flushes all databases.
func (database *StandaloneDatabase) flushAll() resp.Reply {
	for i := range database.dbSet {
		database.flushDB(i)
	}
	return reply.NewOKReply()
}

// flushDB flushes the selected database
func (database *StandaloneDatabase) flushDB(dbIndex int) resp.Reply {
	if dbIndex >= len(database.dbSet) || dbIndex < 0 {
		return reply.NewErrReply("DB index is out of range")
	}
	database.setDB(dbIndex, newDB(dbIndex))
	return reply.NewOKReply()
}

func (database *StandaloneDatabase) setDB(dbIndex int, newDB *DB) resp.Reply {
	if dbIndex >= len(database.dbSet) || dbIndex < 0 {
		return reply.NewErrReply("DB index is out of range")
	}
	oldDB := database.mustSelectDB(dbIndex)
	newDB.index = dbIndex
	newDB.append = oldDB.append // inherit oldDB
	database.dbSet[dbIndex] = newDB
	return reply.NewOKReply()
}
