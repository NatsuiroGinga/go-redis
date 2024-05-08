package cluster_database

import (
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// execFunc is the function to execute command
type execFunc func(cluster *ClusterDatabase, connection resp.Connection, args db.CmdLine) resp.Reply

// newRouter returns a router map for commands
func newRouter() (routerMap map[string]execFunc) {
	cmdMap := map[string]execFunc{
		// enum.RENAMENX.String():     execRenameNx,
		enum.TCC_PREPARE.String():  execPrepare,
		enum.TCC_COMMIT.String():   execCommit,
		enum.TCC_ROLLBACK.String(): execRollback,
	}

	return cmdMap
}

func registerRouter(command *enum.Command, execF execFunc) {
	router[command.String()] = execF
}

// defaultFunc is the default function for commands, it will relay the command to the peer
//
// exp.
//
//	> SET key value
//	< OK
func defaultFunc(clusterDatabase *ClusterDatabase, connection resp.Connection, args db.CmdLine) resp.Reply {
	if len(args) == 0 {
		return reply.NewUnknownErrReply()
	}
	if len(args) == 1 {
		return reply.NewArgNumErrReply(utils.Bytes2String(args[0]))
	}
	key := utils.Bytes2String(args[1])
	peer := clusterDatabase.peerPicker.Pick(key)

	r := clusterDatabase.relay(peer, connection, args)
	return r
}

func init() {
	for _, cmd := range defaultCmds {
		router[cmd] = defaultFunc
	}
	registerRouter(enum.MULTI_KEYS, genPenetratingExecutor(enum.KEYS.String()))
}

var defaultCmds = []string{
	enum.EXPIRE.String(),
	enum.EXPIREAT.String(),
	enum.PEXPIRE.String(),
	enum.PEXPIREAT.String(),
	enum.TTL.String(),
	enum.PTTL.String(),
	enum.PERSIST.String(),
	enum.TYPE.String(),
	enum.SET.String(),
	enum.SETNX.String(),
	"setEx",
	"pSetEx",
	enum.GET.String(),
	"getEx",
	"getSet",
	"getDel",
	enum.INCR.String(),
	"incrBy",
	"incrByFloat",
	enum.DECR.String(),
	"decrBy",
	enum.LPUSH.String(),
	enum.LPUSHX.String(),
	enum.RPUSH.String(),
	enum.RPUSHX.String(),
	enum.LPOP.String(),
	enum.RPOP.String(),
	enum.LREM.String(),
	enum.LLEN.String(),
	enum.LINDEX.String(),
	enum.LSET.String(),
	enum.LRANGE.String(),
	enum.HSET.String(),
	enum.HSETNX.String(),
	enum.HGET.String(),
	enum.HEXISTS.String(),
	enum.HDEL.String(),
	enum.HLEN.String(),
	"HStrLen",
	enum.HMGET.String(),
	enum.HMSET.String(),
	enum.HKEYS.String(),
	enum.HVALS.String(),
	enum.HGETALL.String(),
	"HIncrBy",
	"HIncrByFloat",
	"HRandField",
	enum.SADD.String(),
	enum.SISMEMBER.String(),
	enum.SREM.String(),
	enum.SPOP.String(),
	enum.SCARD.String(),
	enum.SMEMBERS.String(),
	// enum.SINTER.String(),
	// enum.SINTERSTORE.String(),
	// enum.SUNION.String(),
	// enum.SUNIONSTORE.String(),
	// enum.SDIFF.String(),
	// enum.SDIFFSTORE.String(),
	enum.SRANDMEMBER.String(),
	enum.ZADD.String(),
	enum.ZSCORE.String(),
	enum.ZINCRBY.String(),
	enum.ZRANK.String(),
	enum.ZCOUNT.String(),
	enum.ZREVRANK.String(),
	enum.ZCARD.String(),
	enum.ZRANGE.String(),
	enum.ZREVRANGE.String(),
	enum.ZRANGEBYSCORE.String(),
	enum.ZREVRANGEBYSCORE.String(),
	enum.ZREM.String(),
	enum.ZREMRANGEBYSCORE.String(),
	enum.ZREMRANGEBYRANK.String(),
}

// genPenetratingExecutor generates an executor that can reach directly to the database layer
func genPenetratingExecutor(realCmd string) execFunc {
	return func(cluster *ClusterDatabase, c resp.Connection, cmdLine db.CmdLine) resp.Reply {
		cmd := modifyCmd(cmdLine, realCmd)
		logger.Debug("cmd:", utils.CmdLine2String(cmd))
		return cluster.db.Exec(c, cmd)
	}
}

func modifyCmd(cmdLine db.CmdLine, newCmd string) db.CmdLine {
	var cmdLine2 db.CmdLine
	cmdLine2 = append(cmdLine2, cmdLine...)
	cmdLine2[0] = []byte(newCmd)
	return cmdLine2
}
