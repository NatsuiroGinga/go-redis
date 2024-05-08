package cluster_database

import (
	"go-redis/database"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// flushDB removes all data in current database
func flushDB(cluster *ClusterDatabase, conn resp.Connection, cmdLine db.CmdLine) resp.Reply {
	replies := cluster.broadcast(conn, cmdLine)
	var errReply resp.ErrorReply
	for _, v := range replies {
		if reply.IsErrReply(v) {
			errReply = v.(resp.ErrorReply)
			break
		}
	}
	if errReply == nil {
		return reply.NewOKReply()
	}
	return &reply.NormalErrReply{Status: "error occurs: " + errReply.Error()}
}

// execFlushAll removes all data in cluster
func execFlushAll(cluster *ClusterDatabase, conn resp.Connection, args db.CmdLine) resp.Reply {
	return flushDB(cluster, conn, args)
}

// execExists 执行exists命令, 从存储各key的节点中获取数据
func execExists(cluster *ClusterDatabase, conn resp.Connection, args db.CmdLine) resp.Reply {
	if !database.ValidateArity(enum.EXISTS.Arity(), args) {
		return reply.NewArgNumErrReply(enum.EXISTS.String())
	}
	// 1. 取出keys
	keys := utils.CmdLine2Strings(args[1:])
	// 2. 倒排索引
	groupMap := cluster.groupBy(keys)
	if len(groupMap) == 1 {
		for peer := range groupMap {
			return cluster.relay(peer, conn, args)
		}
	}
	// 3. 发送命令给各节点, 计算存在的key的数量
	var errReply resp.Reply
	counter := int64(0)
	for peer, group := range groupMap {
		peerArgs := make([]string, len(group)+1)
		peerArgs[0] = enum.EXISTS.String()
		for i, k := range group {
			peerArgs[i+1] = k
		}
		// 3.1 发送命令
		r := cluster.relay(peer, conn, utils.ToCmdLine(peerArgs...))
		if reply.IsErrReply(r) {
			errReply = r
			break
		}
		// 3.2 处理结果, 累加计数器
		if intReply, ok := r.(*reply.IntReply); !ok {
			errReply = reply.NewErrReply("reply is not IntReply")
			break
		} else {
			counter += intReply.Code()
		}
	}

	if errReply != nil {
		return errReply
	}

	return reply.NewIntReply(counter)
}

// execKeys 把命令发送到所有节点中统计符合条件的key的数量
func execKeys(cluster *ClusterDatabase, conn resp.Connection, args db.CmdLine) resp.Reply {
	// 1. 校验参数合法性
	if !database.ValidateArity(enum.KEYS.Arity(), args) {
		return reply.NewArgNumErrReply(enum.KEYS.String())
	}
	// 2. 转发指令给所有节点
	replies := cluster.broadcast(conn, args)
	keys := make([]string, 0)
	// 3. 处理结果列表
	var errReply resp.ErrorReply
	for _, r := range replies {
		// 3.1 判断是否为错误回复
		if reply.IsErrReply(r) {
			errReply = r.(resp.ErrorReply)
			break
		}
		logger.Debug("reply:", string(r.Bytes()))
		// 3.2 处理多行回复
		/*	if multiReply, ok := r.(*reply.MultiBulkReply); !ok {
				keys = append(keys, utils.Bytes2String(r.(*reply.BulkReply).Arg))
				break
			} else {
				keys = append(keys, utils.CmdLine2Strings(multiReply.Args)...)
			}*/
		switch re := r.(type) {
		case *reply.MultiBulkReply:
			keys = append(keys, utils.CmdLine2Strings(re.Args)...)
		case *reply.BulkReply:
			keys = append(keys, string(re.Arg))
		}
	}
	if errReply != nil {
		return errReply
	}
	return reply.NewMultiBulkReply(utils.ToCmdLine(keys...))
}

func init() {
	registerRouter(enum.FLUSHDB, execFlushDB)
	registerRouter(enum.FLUSHALL, execFlushAll)
	registerRouter(enum.EXISTS, execExists)
	registerRouter(enum.KEYS, execKeys)
}
