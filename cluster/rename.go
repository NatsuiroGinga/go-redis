package cluster_database

import (
	"strconv"

	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// execRename 执行重命名操作, 需要分布式事务支持, 两个步骤:
//
// 1. 删除原有的key
//
// 2. 把原key的值存储到新key中
func execRename(clusterDatabase *ClusterDatabase, connection resp.Connection, args db.CmdLine) resp.Reply {
	// 1. 参数校验
	if len(args) != 3 {
		return reply.NewArgNumErrReply(enum.RENAME.String())
	}
	// 2. 取出新key和旧key以及存储它们的节点
	srcKey := utils.Bytes2String(args[1])
	destKey := utils.Bytes2String(args[2])
	srcNode := clusterDatabase.peerPicker.Pick(srcKey)
	destNode := clusterDatabase.peerPicker.Pick(destKey)
	if srcNode == destNode { // 2.1 新key的旧key的节点是同一个, 直接执行
		return clusterDatabase.relay(srcNode, connection, args)
	}
	// 3. 倒排索引, node -> keys
	groupMap := map[string][]string{
		srcNode:  {srcKey},
		destNode: {destKey},
	}
	// 4. 生成事务id
	txID := clusterDatabase.idGenerator.NextID()
	txIDStr := strconv.FormatInt(txID, 10)
	// 5. 删除原key之前先上写锁, 并且保存原key对应的值
	srcPrepareResp := clusterDatabase.relay(srcNode, connection, makeArgs(enum.TCC_PREPARE.String(), txIDStr,
		enum.RENAMEFROM.String(), srcKey))

	// 6. 如果删除操作会失败, 回滚
	if reply.IsErrReply(srcPrepareResp) {
		requestRollback(clusterDatabase, connection, txID, map[string][]string{srcNode: {srcKey}})
		return srcPrepareResp
	}
	srcPrepareMBR, ok := srcPrepareResp.(*reply.MultiBulkReply)
	if !ok || len(srcPrepareMBR.Args) < 2 {
		requestRollback(clusterDatabase, connection, txID, map[string][]string{srcNode: {srcKey}})
		return reply.NewErrReply("invalid prepare response")
	}
	// 7. 保存新key时上写锁
	destCmd := utils.ToCmdLine2(enum.TCC_PREPARE.String(), utils.String2Bytes(txIDStr),
		enum.RENAMETO.Bytes(), utils.String2Bytes(destKey), srcPrepareMBR.Args[0], srcPrepareMBR.Args[1])
	var destPrepareResp resp.Reply
	destPrepareResp = clusterDatabase.relay(destNode,
		connection, destCmd)

	if reply.IsErrReply(destPrepareResp) {
		requestRollback(clusterDatabase, connection, txID, groupMap)
		return destPrepareResp
	}
	if _, errReply := requestCommit(clusterDatabase, connection, txID, groupMap); errReply != nil {
		requestRollback(clusterDatabase, connection, txID, groupMap)
		return errReply
	}
	return reply.NewOKReply()
}

// prepareRenameFrom 检查准备改名的旧key是否存在, 如果不存在返回错误
func prepareRenameFrom(cluster *ClusterDatabase, conn resp.Connection, cmdLine db.CmdLine) resp.Reply {
	// 1. 参数校验
	if len(cmdLine) != enum.RENAMEFROM.Arity() {
		return reply.NewArgNumErrReply(enum.RENAMEFROM.String())
	}
	// 2. 检查旧key是否存在
	key := utils.Bytes2String(cmdLine[1])
	existResp := cluster.db.ExecWithoutLock(conn, utils.ToCmdLine(enum.EXISTS.String(), key))
	if reply.IsErrReply(existResp) {
		return existResp
	}
	existIntResp := existResp.(*reply.IntReply)
	if existIntResp.Code() == 0 {
		return reply.NewNoSuchKeyErrReply()
	}
	// 3. 把旧key的值序列化并返回
	return cluster.db.ExecWithoutLock(conn, utils.ToCmdLine(enum.DUMPKEY.String(), key))
}

func init() {
	registerPrepareFunc(enum.RENAMEFROM.String(), prepareRenameFrom)
}

// renameCrossSlot is the function for execRename command, it will execRename the key from one slot to another
func renameCrossSlot(clusterDatabase *ClusterDatabase, _ resp.Connection, oldKey, newKey, src, dst string) resp.Reply {
	// 1. get the value of the key
	getCmd := utils.ToCmdLine(enum.GET.String(), oldKey)
	srcClient, err := clusterDatabase.getPeerClient(src)
	if err != nil {
		return reply.NewErrReply(err.Error())
	}
	getReply := srcClient.Send(getCmd)
	if reply.IsErrReply(getReply) {
		return getReply
	}
	bulkReply, ok := getReply.(*reply.BulkReply)
	if !ok {
		return getReply
	}

	// 2. set the value to the new key
	setCmd := utils.ToCmdLine2(enum.SET.String(), utils.String2Bytes(newKey), bulkReply.Arg)
	client, err := clusterDatabase.getPeerClient(dst)
	if err != nil {
		return reply.NewErrReply(err.Error())
	}
	setReply := client.Send(setCmd)
	if reply.IsErrReply(setReply) {
		return setReply
	}

	// 3. delete the old key
	delCmd := utils.ToCmdLine(enum.DEL.String(), oldKey)
	delReply := srcClient.Send(delCmd)
	if reply.IsErrReply(delReply) {
		return delReply
	}

	return reply.NewOKReply()
}

func init() {
	registerRouter(enum.RENAME, execRename)
}
