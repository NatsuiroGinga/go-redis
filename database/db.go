package database

import (
	"strings"
	"time"

	"go-redis/config"
	"go-redis/datastruct/dict"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/lib/timewheel"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// execFunc 是执行命令的函数
//
// params: 不包括命令名的参数
//
// 返回resp格式的回复
type execFunc func(db *DB, params db.Params) resp.Reply

// preFunc 是执行命令之前的函数, 分析命令需要的key
//
// params: 不包括命令名的参数
//
// 返回写键和读键
type preFunc func(params db.Params) (writeKeys, readKeys []string)

// undoFunc 获取给定命令的undo log
type undoFunc func(database *DB, args db.Params) []db.CmdLine

// DB 是Redis使用的底层数据库实现
type DB struct {
	index      int              // 正在使用的数据库号
	data       dict.Dict        // key : entity
	ttl        dict.Dict        // key : expireTime
	versionMap dict.Dict        // key : version
	append     func(db.CmdLine) // 添加一行命令到aof文件
}

// Exec executes command within one database
func (d *DB) Exec(conn resp.Connection, cmd db.CmdLine) resp.Reply {
	// transaction control commands and other commands which cannot execute within transaction
	cmdName := strings.ToLower(utils.Bytes2String(cmd[0]))

	switch cmdName {
	case enum.MULTI:
		if len(cmd) != 1 {
			return reply.NewArgNumErrReply(cmdName)
		}
		return StartMulti(conn)
	case enum.DISCARD:
		if len(cmd) != 1 {
			return reply.NewArgNumErrReply(cmdName)
		}
		return DiscardMulti(conn)
	case enum.EXEC:
		if len(cmd) != 1 {
			return reply.NewArgNumErrReply(cmdName)
		}
		return execMulti(d, conn)
	case enum.WATCH: // 执行watch命令
		if !validateArity(-2, cmd) {
			return reply.NewArgNumErrReply(cmdName)
		}
		return Watch(d, conn, cmd[1:])
	}
	// 如果开始事务, 那么命令加入事务队列
	if conn != nil && conn.InMultiState() {
		return EnqueueCmd(conn, cmd)
	}
	return d.execWithLock(cmd)
}

func execMulti(d *DB, conn resp.Connection) resp.Reply {
	if !conn.InMultiState() {
		return reply.NewErrReply("EXEC without MULTI")
	}
	defer conn.SetMultiState(false)
	if len(conn.GetTxErrors()) > 0 {
		return &reply.NormalErrReply{Status: "EXECABORT Transaction discarded because of previous errors."}
	}
	return d.ExecMulti(conn)
}

// Watch 在执行multi之前，先执行watch key1 [key2 …]，可以监视一个或者多个key
//
// 若在事务的exec命令之前这些key对应的值被其他命令所改动了，那么事务中所有命令都将被打断，即事务所有操作将被取消执行。
func Watch(d *DB, conn resp.Connection, keys db.Params) resp.Reply {
	watching := conn.GetWatching()
	for _, key := range keys {
		keyStr := utils.Bytes2String(key)
		watching[keyStr] = d.GetVersion(keyStr)
	}
	return reply.NewOKReply()
}

// ExecMulti 执行事务中的多个命令
//
// 在multi使用之前要使用watch给键设置版本号
func (d *DB) ExecMulti(conn resp.Connection) resp.Reply {
	// 1. 准备事务中的命令会进行读写的键
	writeKeys := make([]string, 0)
	readKeys := make([]string, 0)
	cmdLines := conn.GetQueuedCmdLine()

	for _, cmdLine := range cmdLines {
		cmdName := strings.ToLower(utils.Bytes2String(cmdLine[0]))
		cmd := cmdTable[cmdName]
		writes, reads := cmd.prepare(cmdLine[1:])
		writeKeys = append(writeKeys, writes...)
		readKeys = append(readKeys, reads...)
	}
	// 2. 对要观察的键设置读锁
	watching := conn.GetWatching()
	for key := range watching {
		readKeys = append(readKeys, key)
	}
	// 3. 上锁
	d.RWLocks(writeKeys, readKeys)
	defer d.RWUnLocks(writeKeys, readKeys)
	// 4. 如果观察的键的版本号变化了, 舍弃此事务
	if isWatchingChanged(d, watching) {
		return reply.NewEmptyMultiBulkReply()
	}
	// 5. 执行事务中的所有命令, 同时保存undo log
	results := make([]resp.Reply, 0, len(cmdLines))
	aborted := false // 标记事务是否执行成功
	undoCmdLines := make([][]db.CmdLine, 0, len(cmdLines))
	for _, cmdLine := range cmdLines {
		// 5.1 在命令执行前, 保存undo log
		undoCmdLines = append(undoCmdLines, d.GetUndoLogs(cmdLine))
		// 5.2 保存一条命令的执行结果
		result := d.exec(cmdLine)
		// 5.3 如果命令的结果是错误
		if reply.IsErrReply(result) {
			// 5.3.1 标记事务执行失败
			aborted = true
			// 5.3.2 执行出错的命令要从undo log中删除
			undoCmdLines = undoCmdLines[:len(undoCmdLines)-1]
			break
		}
		results = append(results, result)
	}
	// 6. 如果事务执行成功, 把修改的key的版本号加1
	if !aborted {
		d.addVersion(writeKeys...)
		return reply.NewMultiRawReply(results)
	}
	// undo if aborted
	size := len(undoCmdLines)
	for i := size - 1; i >= 0; i-- {
		curCmdLines := undoCmdLines[i]
		if len(curCmdLines) == 0 {
			continue
		}
		for _, cmdLine := range curCmdLines {
			d.exec(cmdLine)
		}
	}
	return &reply.NormalErrReply{Status: "EXECABORT Transaction discarded because of previous errors."}
}

// isWatchingChanged 判断链接持有的关键字-版本号 和 底层数据库存储的关键字-版本号 是否一致, 并发不安全
//
// 如果不一致返回false, 一致则返回true
func isWatchingChanged(d *DB, watching map[string]uint32) bool {
	for key, ver := range watching {
		currentVersion := d.GetVersion(key)
		if ver != currentVersion {
			return true
		}
	}
	return false
}

// DiscardMulti 用来取消一个事务
func DiscardMulti(conn resp.Connection) resp.Reply {
	if !conn.InMultiState() {
		return reply.NewErrReply("DISCARD without MULTI")
	}
	conn.ClearQueuedCmds()
	conn.SetMultiState(false)
	return reply.NewOKReply()
}

// StartMulti 用来组装一个事务
//
// 从输入Multi命令开始，输入的命令都会依次进入命令队列中，但不会执行
//
// 直到输入Exec后，redis会将之前的命令依次执行。
func StartMulti(conn resp.Connection) resp.Reply {
	if conn.InMultiState() {
		return reply.NewErrReply("MULTI calls can not be nested")
	}
	conn.SetMultiState(true)
	return reply.NewOKReply()
}

// execWithLock 执行命令
func (d *DB) execWithLock(cmd db.CmdLine) resp.Reply {
	if len(cmd) == 0 {
		return reply.NewNoReply()
	}
	// 1. 取出命令, 例如: set, get 或者其他
	instruction := strings.ToLower(utils.Bytes2String(cmd[0]))
	// 2. 根据命令字符串取出执行命令的具体实例
	com, ok := cmdTable[instruction]
	if !ok {
		return reply.NewUnknownCommandErrReply(instruction)
	}
	// 3. 检查参数数量合法性
	if !validateArity(com.arity, cmd) {
		return reply.NewArgNumErrReply(instruction)
	}
	// 4. 给要处理的键上读/写锁
	writeKeys, readKeys := com.prepare(cmd[1:])
	d.RWLocks(writeKeys, readKeys)
	defer d.RWUnLocks(writeKeys, readKeys)

	return com.executor(d, cmd[1:])
}

// exec 执行命令，并发不安全
func (d *DB) exec(cmd db.CmdLine) resp.Reply {
	if len(cmd) == 0 {
		return reply.NewNoReply()
	}
	// 1. 取出命令, 例如: set, get 或者其他
	instruction := strings.ToLower(utils.Bytes2String(cmd[0]))
	// 2. 根据命令字符串取出执行命令的具体实例
	com, ok := cmdTable[instruction]
	if !ok {
		return reply.NewUnknownCommandErrReply(instruction)
	}
	// 3. 检查参数数量合法性
	if !validateArity(com.arity, cmd) {
		return reply.NewArgNumErrReply(instruction)
	}
	return com.executor(d, cmd[1:])
}

/*
处理DataEntity的方法
包括: get, set, remove, putIfExists, putIfAbsent
*/

// Remove 删除一个key以及它的ttl
func (d *DB) Remove(key string) {
	// 1. 删除数据
	d.data.Remove(key)
	// 2. 删除过期
	d.ttl.Remove(key)
	// 3. 取消执行过期删除的任务
	taskKey := getExpireTaskKey(key)
	timewheel.Cancel(taskKey)
}

// removes 删除数据库中的key
//
// 返回删除掉的key的数量
func (d *DB) removes(keys ...string) (n int) {
	for _, key := range keys {
		_, ok := d.data.Get(key)
		if ok {
			d.Remove(key)
			n++
		}
	}

	return n
}

// getEntity 返回key的对应的dataEntity. 并发不安全
//
// 返回nil, false代表key不存在
func (d *DB) getEntity(key string) (entity *db.DataEntity, ok bool) {
	// 1. 检查key是否存在
	value, isExist := d.data.Get(key)
	if !isExist {
		return nil, false
	}
	// 2. 检查是否过期, 惰性删除
	if d.expireIfNeeded(key) {
		return nil, false
	}
	// 3. 取出entity
	entity = value.(*db.DataEntity)

	return entity, true
}

// putEntity 向数据库中放入key-value. 并发不安全
//
// 返回放入的key-value的数量
func (d *DB) putEntity(key string, entity *db.DataEntity) (n int) {
	return d.data.Set(key, entity)
}

// putIfExists 如果key存在, 放入key-value, 否则不做任何操作. 并发不安全
//
// 返回放入的key-value的数量
func (d *DB) putIfExists(key string, entity *db.DataEntity) (n int) {
	// 1. 如果key过期了, 就返回0
	if d.expireIfNeeded(key) {
		return 0
	}
	// 2. 否则修改key-value
	return d.data.PutIfExist(key, entity)
}

// putIfAbsent 如果key不存在, 放入key-value, 否则不做任何操作. 并发不安全
//
// 返回放入的key-value的数量
func (d *DB) putIfAbsent(key string, entity *db.DataEntity) (n int) {
	return d.data.PutIfAbsent(key, entity)
}

// RWLocks lock keys for writing and reading
func (d *DB) RWLocks(writeKeys, readKeys []string) {
	d.data.RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (d *DB) RWUnLocks(writeKeys, readKeys []string) {
	d.data.RWUnLocks(writeKeys, readKeys)
}

// 有关TTL的方法
// 包括: Expire, Persist, ExpireIfNeeded, IsExpired

// expire 给已存在的key设置过期时间, 并发安全
func (d *DB) expire(key string, expireTime time.Time) {
	// 1. 给key设置过期时间
	d.ttl.SetWithLock(key, expireTime)
	// 2. 设置时间轮的任务
	taskKey := getExpireTaskKey(key)
	timewheel.At(expireTime, taskKey, func() {
		// 2.1 给key上写锁, 防止其他协程删除key
		keys := []string{key}

		d.RWLocks(keys, nil)
		defer d.RWUnLocks(keys, nil)

		logger.Info("expire", key)
		// 2.2 双重检查, 防止key的ttl更新
		d.expireIfNeeded(key)
	})
}

// persist 取消key的ttl, 并发安全
func (d *DB) persist(key string) {
	d.ttl.RemoveWithLock(key)
	taskKey := getExpireTaskKey(key)
	timewheel.Cancel(taskKey)
}

// expireIfNeeded 检查key是否过期, 如果过期了就删除, 并发安全
//
// 如果key过期了就删除然后返回true, key不存在、没有过期时间 或者 有过期时间但是没过期 返回false
func (d *DB) expireIfNeeded(key string) bool {
	t, exist := d.ttl.GetWithLock(key)
	if !exist {
		return false
	}
	expireTime := t.(time.Time)
	isExpire := time.Now().After(expireTime)
	if isExpire {
		d.Remove(key)
	}
	return isExpire
}

// validateArity 验证输入的命令参数是否与设定的命令的参数数量一致
//
// 如果命令是可变长参数, 则返回len(cmd) >= -arity的结果, 否则返回len(cmd) == arity的结果
func validateArity(arity int, cmd db.CmdLine) bool {
	argNum := len(cmd)
	if arity > 0 {
		return arity == argNum
	}
	return argNum >= -arity
}

// getExpireTask 获得过期任务的名字
func getExpireTaskKey(key string) string {
	return "expire-" + key
}

func (d *DB) Flush() {
	d.data.Clear()
	d.ttl.Clear()
	// d.versionMap.Clear()
}

// GetVersion 获取key的version, 并发不安全
func (d *DB) GetVersion(key string) uint32 {
	entity, ok := d.versionMap.Get(key)
	if !ok {
		return 0
	}
	return entity.(uint32)
}

// addVersion 把key的版本号加1, 并发不安全
func (d *DB) addVersion(keys ...string) {
	for _, key := range keys {
		versionCode := d.GetVersion(key)
		d.versionMap.Set(key, versionCode+1)
	}
}

// newDB creates a new database with the given index.
func newDB(index int) *DB {
	return &DB{index,
		dict.NewConcurrentDict(config.Properties.Buckets),
		dict.NewConcurrentDict(config.Properties.Buckets >> 6),
		dict.NewConcurrentDict(config.Properties.Buckets),
		func(db.CmdLine) {},
	}
}
