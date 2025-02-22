package cluster_database

import (
	"strconv"

	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// execMSet 在多个节点中设置多个key-value, 会使用分布式事务确保数据一致性
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
		keys[i] = utils.Bytes2String(cmdLine[2*i+1])
		valueMap[keys[i]] = utils.Bytes2String(cmdLine[2*i+2])
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
	// 5. 判断是否需要回滚
	if rollback {
		// 5.1 回滚
		requestRollback(cluster, conn, txID, groupMap)
	} else {
		// 5.2 提交
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
