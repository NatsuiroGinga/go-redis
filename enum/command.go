package enum

import (
	"go-redis/lib/utils"
)

// Command 命令枚举
type Command struct {
	name       string // 命令名称
	paramCount int    // 除去命令本身后的参数数量
}

func (cmd *Command) Bytes() []byte {
	return utils.String2Bytes(cmd.name)
}

// Name 返回命令名称
func (cmd *Command) Name() string {
	return cmd.name
}

// String equals to Name().
func (cmd *Command) String() string {
	return cmd.name
}

// ParamCount 返回命令参数数量
func (cmd *Command) ParamCount() int {
	return cmd.paramCount
}

// Arity 返回命令带命令本身的参数数量, 即 ParamCount() + 1
func (cmd *Command) Arity() int {
	return utils.If(cmd.paramCount >= 0, cmd.paramCount+1, cmd.paramCount-1)
}

// db command
var (
	FLUSHALL = &Command{"FLUSHALL", 0}
	PING     = &Command{name: "PING", paramCount: 0}
	SELECT   = &Command{name: "SELECT", paramCount: 1}
)

// keys command
var (
	DEL         = &Command{name: "DEL", paramCount: -1}
	EXISTS      = &Command{name: "EXISTS", paramCount: -1}
	FLUSHDB     = &Command{name: "FLUSHDB", paramCount: 0}
	TYPE        = &Command{name: "TYPE", paramCount: 1}
	RENAME      = &Command{name: "RENAME", paramCount: 2}
	RENAMENX    = &Command{name: "RENAMENX", paramCount: 2}
	KEYS        = &Command{name: "KEYS", paramCount: 1}
	EXPIRE      = &Command{name: "EXPIRE", paramCount: 2}
	EXPIREAT    = &Command{name: "EXPIREAT", paramCount: 2}
	EXPIRETIME  = &Command{name: "EXPIRETIME", paramCount: 1}
	TTL         = &Command{name: "TTL", paramCount: 1}
	PEXPIRE     = &Command{name: "PEXPIRE", paramCount: 2}
	PEXPIREAT   = &Command{name: "PEXPIREAT", paramCount: 2}
	PEXPIRETIME = &Command{name: "PEXPIRETIME", paramCount: 1}
	PTTL        = &Command{name: "PTTL", paramCount: 1}
	PERSIST     = &Command{name: "PERSIST", paramCount: 2}
)

// string command
var (
	GET    = &Command{name: "GET", paramCount: 1}
	SET    = &Command{name: "SET", paramCount: 2}
	MSET   = &Command{name: "MSET", paramCount: -2}
	MGET   = &Command{name: "MGET", paramCount: -1}
	SETNX  = &Command{name: "SETNX", paramCount: 2}
	STRLEN = &Command{name: "STRLEN", paramCount: 1}
	GETSET = &Command{name: "GETSET", paramCount: 2}
	INCR   = &Command{name: "INCR", paramCount: 1}
	DECR   = &Command{name: "DECR", paramCount: 1}
)

// zset command
var (
	ZADD             = &Command{name: "ZADD", paramCount: -4}
	ZSCORE           = &Command{name: "ZSCORE", paramCount: 2}
	ZINCRBY          = &Command{name: "ZINCRBY", paramCount: 3}
	ZRANK            = &Command{name: "ZRANK", paramCount: 2}
	ZCOUNT           = &Command{name: "ZCOUNT", paramCount: 3}
	ZREVRANK         = &Command{name: "ZREVRANK", paramCount: 2}
	ZCARD            = &Command{name: "ZCARD", paramCount: 1}
	ZRANGE           = &Command{name: "ZRANGE", paramCount: -3}
	ZRANGEBYSCORE    = &Command{name: "ZRANGEBYSCORE", paramCount: -3}
	ZREVRANGE        = &Command{name: "ZREVRANGE", paramCount: -3}
	ZREVRANGEBYSCORE = &Command{name: "ZREVRANGEBYSCORE", paramCount: -3}
	ZPOPMIN          = &Command{name: "ZPOPMIN", paramCount: -1}
	ZPOPMAX          = &Command{name: "ZPOPMAX", paramCount: -1}
	ZREM             = &Command{name: "ZREM", paramCount: -2}
	ZREMRANGEBYSCORE = &Command{name: "ZREMRANGEBYSCORE", paramCount: 3}
	ZREMRANGEBYRANK  = &Command{name: "ZREMRANGEBYRANK", paramCount: 3}
)

// list command
var (
	LPUSH     = &Command{name: "LPUSH", paramCount: -2}
	LPUSHX    = &Command{name: "LPUSHX", paramCount: -2}
	RPUSH     = &Command{name: "RPUSH", paramCount: -2}
	RPUSHX    = &Command{name: "RPUSHX", paramCount: -2}
	LPOP      = &Command{name: "LPOP", paramCount: 1}
	RPOP      = &Command{name: "RPOP", paramCount: 1}
	RPOPLPUSH = &Command{name: "RPOPLPUSH", paramCount: 2}
	LREM      = &Command{name: "LREM", paramCount: 3}
	LLEN      = &Command{name: "LLEN", paramCount: 1}
	LINDEX    = &Command{name: "LINDEX", paramCount: 2}
	LSET      = &Command{name: "LSET", paramCount: 3}
	LRANGE    = &Command{name: "LRANGE", paramCount: 3}
	LTRIM     = &Command{name: "LTRIM", paramCount: 3}
	LINSERT   = &Command{name: "LINSERT", paramCount: 4}
)

// hash command
var (
	HDEL    = &Command{name: "HDEL", paramCount: -2}
	HEXISTS = &Command{name: "HEXISTS", paramCount: 2}
	HGET    = &Command{name: "HGET", paramCount: 2}
	HGETALL = &Command{name: "HGETALL", paramCount: 1}
	HKEYS   = &Command{name: "HKEYS", paramCount: 1}
	HLEN    = &Command{name: "HLEN", paramCount: 1}
	HMGET   = &Command{name: "HMGET", paramCount: -2}
	HSET    = &Command{name: "HSET", paramCount: 3}
	HMSET   = &Command{name: "HMSET", paramCount: -3}
	HSETNX  = &Command{name: "HSETNX", paramCount: 3}
	HVALS   = &Command{name: "HVALS", paramCount: 1}
)

// set command
var (
	SADD        = &Command{name: "SADD", paramCount: -2}
	SCARD       = &Command{name: "SCARD", paramCount: 1}
	SDIFF       = &Command{name: "SDIFF", paramCount: -1}
	SDIFFSTORE  = &Command{name: "SDIFFSTORE", paramCount: -2}
	SINTER      = &Command{name: "SINTER", paramCount: -1}
	SINTERSTORE = &Command{name: "SINTERSTORE", paramCount: -2}
	SISMEMBER   = &Command{name: "SISMEMBER", paramCount: 2}
	SMEMBERS    = &Command{name: "SMEMBERS", paramCount: 1}
	SMOVE       = &Command{name: "SMOVE", paramCount: 3}
	SPOP        = &Command{name: "SPOP", paramCount: -1}
	SRANDMEMBER = &Command{name: "SRANDMEMBER", paramCount: -1}
	SREM        = &Command{name: "SREM", paramCount: -2}
	SUNION      = &Command{name: "SUNION", paramCount: -1}
	SUNIONSTORE = &Command{name: "SUNIONSTORE", paramCount: -2}
)

// transactions command
var (
	// local
	TX_MULTI   = &Command{name: "MULTI", paramCount: 0}
	TX_EXEC    = &Command{name: "EXEC", paramCount: 0}
	TX_DISCARD = &Command{name: "DISCARD", paramCount: 0}
	TX_WATCH   = &Command{name: "WATCH", paramCount: -1}
	TX_UNWATCH = &Command{name: "UNWATCH", paramCount: 0}
	// cluster
	TCC_PREPARE  = &Command{name: "PREPARE", paramCount: -2}
	TCC_COMMIT   = &Command{name: "COMMIT", paramCount: 1}
	TCC_ROLLBACK = &Command{name: "ROLLBACK", paramCount: 1}
)

// cluster command
var (
	RENAMEFROM = &Command{name: "RENAMEFROM", paramCount: 1}
	DUMPKEY    = &Command{name: "DUMPKEY", paramCount: 1}
	RENAMETO   = &Command{name: "RENAMETO", paramCount: 3}
)

// system command
var (
	SYS_AUTH = &Command{name: "AUTH", paramCount: 1}
)

// Command flags
const (
	ZSET_WITH_SCORES = "WITHSCORES"
	ZSET_LIMIT       = "LIMIT"
	LIST_BEFORE      = "BEFORE"
	LIST_AFTER       = "AFTER"
)
