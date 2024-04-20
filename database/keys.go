package database

import (
	"strconv"
	"time"

	"go-redis/datastruct/dict"
	"go-redis/datastruct/list"
	"go-redis/datastruct/set"
	"go-redis/datastruct/sortedset"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/lib/wildcard"
	"go-redis/resp/reply"
)

// execDel 删除多个key
//
// # del key1 key2, 例如: del jack john
//
// 返回删除掉的key的数量
func execDel(d *DB, args db.Params) resp.Reply {
	keys := make([]string, 0, len(args))
	for _, arg := range args {
		keys = append(keys, utils.Bytes2String(arg))
	}
	n := d.removes(keys...)

	if n > 0 {
		d.append(utils.ToCmdLine2(enum.DEL.String(), args...))
	}

	return reply.NewIntReply(int64(n))
}

// execExists returns the number of keys existing.
func execExists(d *DB, args db.Params) resp.Reply {
	n := int64(0)

	for _, arg := range args {
		key := utils.Bytes2String(arg)
		_, ok := d.getEntity(key)
		if ok {
			n++
		}
	}

	return reply.NewIntReply(n)
}

// execFlushDB deletes all the keys.
func execFlushDB(d *DB, _ db.Params) resp.Reply {
	d.Flush()
	d.append(utils.ToCmdLine(enum.FLUSHDB.String()))

	return reply.NewOKReply()
}

// execType 判断key对应的值的类型
//
// # type key, 例如: type jack
func execType(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	entity, ok := d.getEntity(key)
	if !ok {
		return reply.NewErrReply("none")
	}

	switch entity.Data.(type) {
	case []byte, string:
		return reply.NewStatusReply(enum.TYPE_STRING.String())
	case sortedset.SortedSet:
		return reply.NewStatusReply(enum.TYPE_ZSET.String())
	case list.List:
		return reply.NewStatusReply(enum.TYPE_LIST.String())
	case dict.Dict:
		return reply.NewStatusReply(enum.TYPE_HASH.String())
	case *set.HashSet:
		return reply.NewStatusReply(enum.TYPE_SET.String())
	default:
		return reply.NewUnknownErrReply()
	}
}

// execRename 重命名一个key
func execRename(d *DB, args db.Params) resp.Reply {
	// 1. 取出原始key
	src := utils.Bytes2String(args[0])
	// 2. 取出目标key
	dst := utils.Bytes2String(args[1])
	// 3. 取出原始key对应的value和ttl
	entity, ok := d.getEntity(src)
	if !ok {
		return reply.NewNoSuchKeyErrReply()
	}
	// 4. 将目标key和原始value放回数据库
	d.putEntity(dst, entity)
	// 5. 删除原始key
	d.removes(src)
	// 6. 如果原始key有ttl, 则将原始ttl设置到目标key
	t, hasTTL := d.ttl.Get(src)
	if hasTTL {
		// 6.1 删除src和dst以及他们的ttl
		d.persist(src)
		d.persist(dst)
		expireTime := t.(time.Time)
		d.expire(dst, expireTime)
	}
	// 7. 添加一个命令到aof文件
	d.append(utils.ToCmdLine2(enum.RENAME.String(), args...))

	return reply.NewOKReply()
}

// execRenameNX 如果目标key不存在, 则把原始key改为目标key, 原始value不变
//
// 如果目标key存在, 返回0, 否则返回1
func execRenameNX(d *DB, args db.Params) resp.Reply {
	// 1. 取出src和dst
	src := utils.Bytes2String(args[0])
	dst := utils.Bytes2String(args[1])
	// 2. 查询dst是否存在
	_, ok := d.getEntity(dst)
	// 3. dst存在则返回0
	if ok {
		return reply.NewIntReply(0)
	}
	// 4. 查询src是否存在
	entity, ok := d.getEntity(src)
	if !ok {
		return reply.NewNoSuchKeyErrReply()
	}
	// 5. 检查src是否有ttl
	t, hasTTL := d.ttl.Get(src)
	// 6. 删除src和dst的key和ttl
	d.removes(src, dst)
	// 7. 放入dst和entity
	d.putEntity(dst, entity)
	// 8. 如果src有ttl, 更新dst的ttl
	if hasTTL {
		expireTime := t.(time.Time)
		d.expire(dst, expireTime)
	}
	// 8. 此命令加入aof
	d.append(utils.ToCmdLine2(enum.RENAMENX.String(), args...))

	return reply.NewIntReply(1)
}

// execExpire 设置一个key的存活时间, 单位为s
//
// # expire key ttl, 例如: expire jack 10
//
// 如果设置成功返回1, 否则返回0
func execExpire(d *DB, args db.Params) resp.Reply {
	// 1. 取出key和ttl
	key := utils.Bytes2String(args[0])
	ttlStr := utils.Bytes2String(args[1])
	// 2. 转换ttl到int64范围内的秒
	ttlInt64, err := strconv.ParseInt(ttlStr, 10, 64)
	if err != nil {
		return reply.NewIntErrReply()
	}
	ttl := time.Duration(ttlInt64) * time.Second
	// 3. 查询key是否存在
	_, ok := d.getEntity(key)
	if !ok {
		return reply.NewIntReply(0)
	}
	// 4. 计算过期时间并设置给key
	expireTime := time.Now().Add(ttl)
	d.expire(key, expireTime)
	d.append(utils.ToCmdLine2(enum.EXPIRE.String(), args...))

	return reply.NewIntReply(1)
}

// execExpireAt 设置key的过期时间, 单位为s
//
// # Expireat KEY_NAME TIME_IN_UNIX_TIMESTAMP, 例如: EXPIREAT jack 1293840000
//
// 设置成功返回1, 当 key不存在返回 0.
func execExpireAt(d *DB, args db.Params) resp.Reply {
	// 1. 取出key和timestamp
	key := utils.Bytes2String(args[0])
	expireAtStr := utils.Bytes2String(args[1])
	// 2. 检查key是否存在
	_, ok := d.getEntity(key)
	if !ok {
		return reply.NewIntReply(0)
	}
	// 3. 转换
	expireAtInt64, err := strconv.ParseInt(expireAtStr, 10, 64)
	if err != nil {
		return reply.NewIntErrReply()
	}
	expireAt := time.Unix(expireAtInt64, 0)
	// 4. 设置key的过期时间
	d.expire(key, expireAt)
	d.append(utils.ToCmdLine2(enum.EXPIREAT.String(), args...))

	return reply.NewIntReply(1)
}

// execExpireTime 查看key的过期时间
//
// # expiretime key, 例如: expire jack
//
// 当 key 不存在时，返回 -2. 当 key 存在但没有设置剩余生存时间时，返回 -1. 否则，以秒为单位，返回 key的过期时间的unix时间戳转换的秒
func execExpireTime(d *DB, args db.Params) resp.Reply {
	// 1. 取出key
	key := utils.Bytes2String(args[0])
	// 2. 查看key是否存在
	_, exist := d.getEntity(key)
	if !exist {
		return reply.NewIntReply(-2)
	}
	// 3. 取出key的ttl
	t, exist := d.ttl.Get(key)
	if !exist {
		return reply.NewIntReply(-1)
	}
	expireTime, _ := t.(time.Time)

	// 4. 返回unix时间戳转换成的秒
	return reply.NewIntReply(expireTime.Unix())
}

// execTTL 以秒为单位，返回给定 key 的剩余生存时间(TTL, time to live)
//
// # ttl key, 例如: ttl jack
//
// 当 key 不存在时，返回 -2. 当 key 存在但没有设置剩余生存时间时，返回 -1. 否则，以秒为单位，返回 key 的剩余生存时间
func execTTL(d *DB, args db.Params) resp.Reply {
	// 1. 取出key
	key := utils.Bytes2String(args[0])
	// 2. 检查key是否存在
	_, exist := d.getEntity(key)
	if !exist {
		return reply.NewIntReply(-2)
	}
	// 3. 检查key是否有ttl
	t, hasTTL := d.ttl.Get(key)
	if !hasTTL {
		return reply.NewIntReply(1)
	}
	// 4. 计算key距离过期还有多少秒
	expireTime := t.(time.Time)
	ttl := expireTime.Sub(time.Now()).Seconds()
	return reply.NewIntReply(int64(ttl))
}

// execPExpire 设置key的过期时间(ms)
//
// # pexpire key ttl, 例如: PEXPIRE jack 1500
//
// 设置成功，返回 1; key 不存在或设置失败，返回 0
func execPExpire(d *DB, args db.Params) resp.Reply {
	// 1. 取出key和ttl
	key := utils.Bytes2String(args[0])
	ttlStr := utils.Bytes2String(args[1])
	// 2. 转换ttl到int64范围内的秒
	ttlInt64, err := strconv.ParseInt(ttlStr, 10, 64)
	if err != nil {
		return reply.NewErrReply("value is not an integer or out of range")
	}
	ttl := time.Duration(ttlInt64) * time.Millisecond
	// 3. 查询key是否存在
	_, ok := d.getEntity(key)
	if !ok {
		return reply.NewIntReply(0)
	}
	// 4. 计算过期时间并设置给key
	expireTime := time.Now().Add(ttl)
	d.expire(key, expireTime)
	d.append(utils.ToCmdLine2(enum.PEXPIRE.String(), args...))

	return reply.NewIntReply(1)
}

// execPExpireAt 设置key的过期时间, 单位为ms
//
// # pexpireat KEY_NAME TIME_IN_UNIX_TIMESTAMP, 例如: PEXPIREAT jack 1293840000
//
// 设置成功返回1, 当 key不存在返回 0.
func execPExpireAt(d *DB, args db.Params) resp.Reply {
	// 1. 取出key和timestamp
	key := utils.Bytes2String(args[0])
	expireAtStr := utils.Bytes2String(args[1])
	// 2. 检查key是否存在
	_, ok := d.getEntity(key)
	if !ok {
		return reply.NewIntReply(0)
	}
	// 3. 转换
	expireAtInt64, err := strconv.ParseInt(expireAtStr, 10, 64)
	if err != nil {
		return reply.NewErrReply("value is not an integer or out of range")
	}
	expireAt := time.UnixMilli(expireAtInt64)
	// 4. 设置key的过期时间
	d.expire(key, expireAt)
	d.append(utils.ToCmdLine2(enum.PEXPIREAT.String(), args...))

	return reply.NewIntReply(1)
}

// execPExpireTime 查看key的过期时间
//
// # pexpiretime key, 例如: pexpiretime jack
//
// 当 key 不存在时，返回 -2. 当 key 存在但没有设置剩余生存时间时，返回 -1. 否则，以秒为单位，返回 key的过期时间的unix时间戳转换的毫秒
func execPExpireTime(d *DB, args db.Params) resp.Reply {
	// 1. 取出key
	key := utils.Bytes2String(args[0])
	// 2. 查看key是否存在
	_, exist := d.getEntity(key)
	if !exist {
		return reply.NewIntReply(-2)
	}
	// 3. 取出key的ttl
	t, exist := d.ttl.Get(key)
	if !exist {
		return reply.NewIntReply(-1)
	}
	expireTime, _ := t.(time.Time)

	// 4. 返回unix时间戳转换成的秒
	return reply.NewIntReply(expireTime.UnixMilli())
}

// execPTTL 以毫秒为单位，返回给定 key 的剩余生存时间(TTL, time to live)
//
// # ttl key, 例如: ttl jack
//
// 当 key 不存在时，返回 -2. 当 key 存在但没有设置剩余生存时间时，返回 -1. 否则，以毫秒为单位，返回 key 的剩余生存时间
func execPTTL(d *DB, args db.Params) resp.Reply {
	// 1. 取出key
	key := utils.Bytes2String(args[0])
	// 2. 检查key是否存在
	_, exist := d.getEntity(key)
	if !exist {
		return reply.NewIntReply(-2)
	}
	// 3. 检查key是否有ttl
	t, hasTTL := d.ttl.Get(key)
	if !hasTTL {
		return reply.NewIntReply(1)
	}
	// 4. 计算key距离过期还有多少毫秒
	expireTime := t.(time.Time)
	ttl := expireTime.Sub(time.Now()).Milliseconds()
	return reply.NewIntReply(ttl)
}

// execKeys 查找所有符合通配符模板的key
//
// 例如: 现有a, ab, abc, z四个key, 使用keys a*命令会返回a, ab, abc, 因为以上三个命令符合a*这个模板
func execKeys(d *DB, args db.Params) resp.Reply {
	// 1. 取出key, pattern
	key := string(args[0])
	pattern := wildcard.CompilePattern(key)
	result := make([][]byte, 0)
	// 2. 从数据字典中查询符合pattern且没过期的key
	d.data.ForEach(func(key string, value any) bool {
		// 2.1 检查key是否符合pattern, 检查key是否过期
		if pattern.IsMatch(key) && !d.expireIfNeeded(key) {
			result = append(result, utils.String2Bytes(key))
		}
		return true
	})
	// 3. 返回结果集
	return reply.NewMultiBulkReply(result)
}

func init() {
	registerCommand(enum.DEL, writeAllKeys, execDel)
	registerCommand(enum.EXISTS, readAllKeys, execExists)
	registerCommand(enum.FLUSHDB, noPrepare, execFlushDB)
	registerCommand(enum.TYPE, readFirstKey, execType)
	registerCommand(enum.RENAME, prepareRename, execRename)
	registerCommand(enum.RENAMENX, prepareRename, execRenameNX)
	registerCommand(enum.KEYS, noPrepare, execKeys)
	registerCommand(enum.EXPIRE, writeFirstKey, execExpire)
	registerCommand(enum.EXPIRETIME, readFirstKey, execExpireTime)
	registerCommand(enum.TTL, readFirstKey, execTTL)
	registerCommand(enum.EXPIREAT, writeFirstKey, execExpireAt)
	registerCommand(enum.PEXPIRE, writeFirstKey, execPExpire)
	registerCommand(enum.PEXPIREAT, writeFirstKey, execPExpireAt)
	registerCommand(enum.PEXPIRETIME, readFirstKey, execPExpireTime)
	registerCommand(enum.PTTL, readFirstKey, execPTTL)
}
