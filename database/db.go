package database

import (
	"strings"
	"time"

	"go-redis/config"
	"go-redis/datastruct/dict"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/lib/timewheel"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// execFunc 是执行命令的函数
// params: 不包括命令名的参数
// 返回resp格式的回复
type execFunc func(db *DB, params db.Params) resp.Reply

// preFunc 是执行命令之前的函数, 分析命令需要的key
//
// params: 不包括命令名的参数
type preFunc func(params db.Params) (writeKeys, readKeys []string)

// DB 是Redis使用的底层数据库实现
type DB struct {
	index  int              // 正在使用的数据库号
	data   dict.Dict        // key : entity
	ttl    dict.Dict        // key : expireTime
	append func(db.CmdLine) // 添加一行命令到aof文件
}

// exec 执行命令
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
	// 4. 给要处理的键上读/写锁
	writeKeys, readKeys := com.prepare(db.Params(cmd[1:]))
	d.RWLocks(writeKeys, readKeys)
	defer d.RWUnLocks(writeKeys, readKeys)

	return com.executor(d, db.Params(cmd[1:]))
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
// arity >= 1 或者 arity <= -1
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
}

// newDB creates a new database with the given index.
func newDB(index int) *DB {
	return &DB{index,
		dict.NewConcurrentDict(config.Properties.Buckets),
		dict.NewConcurrentDict(config.Properties.Buckets >> 6),
		func(db.CmdLine) {},
	}
}
