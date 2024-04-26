package cluster_database

import (
	"strconv"

	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// execMSet atomically sets multi key-value in cluster, writeKeys can be distributed on any node
func execMSet(cluster *ClusterDatabase, conn resp.Connection, cmdLine db.CmdLine) resp.Reply {
	// 1. 取出参数
	argCount := len(cmdLine) - 1
	if argCount%2 != 0 || argCount < 1 {
		return reply.NewArgNumErrReply(enum.MSET.String())
	}
	size := argCount / 2
	keys := make([]string, size)
	valueMap := make(map[string]string)
	for i := 0; i < size; i++ {
		keys[i] = string(cmdLine[2*i+1])
		valueMap[keys[i]] = string(cmdLine[2*i+2])
	}
	// 2. 创建node : keys的索引
	groupMap := cluster.groupBy(keys)
	// 3. 如果所有key都存储在一个节点内, 直接执行命令即可
	if len(groupMap) == 1 {
		for peer := range groupMap {
			return cluster.relay(peer, conn, cmdLine)
		}
	}

	// 4. 开始事务之前的准备工作
	var errReply resp.Reply
	txID := cluster.idGenerator.NextID() // 4.1 生成分布式事务id
	txIDStr := strconv.FormatInt(txID, 10)
	rollback := false
	for peer, group := range groupMap { // 4.2 组装参数, 发送给每一个跟此事务相关的节点
		peerArgs := []string{txIDStr, enum.MSET.String()}
		for _, k := range group {
			peerArgs = append(peerArgs, k, valueMap[k])
		}
		// 4.3 发送命令
		r := cluster.relay(peer, conn, makeArgs(enum.TCC_PREPARE.String(), peerArgs...))
		if reply.IsErrReply(r) {
			errReply = r
			rollback = true
			break
		}
	}
	if rollback {
		requestRollback(cluster, conn, txID, groupMap)
	} else {
		_, errReply = requestCommit(cluster, conn, txID, groupMap)
		rollback = errReply != nil
	}
	if !rollback {
		return reply.NewOKReply()
	}
	return errReply
}

func init() {
	registerRouter(enum.MSET, execMSet)
}
