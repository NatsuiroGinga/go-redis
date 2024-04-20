package cluster_database

import (
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

var renamenx = cmdFunc(func(clusterDatabase *ClusterDatabase, connection resp.Connection, args db.CmdLine) resp.Reply {
	if len(args) != 3 {
		return reply.NewArgNumErrReply(enum.RENAMENX.String())
	}
	// 1. check if the newKey exists in the dst node
	newKey := utils.Bytes2String(args[2])
	dst := clusterDatabase.peerPicker.Pick(newKey)

	dstClient, err := clusterDatabase.getPeerClient(dst)
	if err != nil {
		return reply.NewErrReply(err.Error())
	}

	existsCmd := utils.ToCmdLine2(enum.EXISTS.String(), utils.String2Bytes(newKey))
	existsReply := dstClient.Send(existsCmd)

	if reply.IsErrReply(existsReply) {
		return existsReply
	}

	intReply, ok := existsReply.(*reply.IntReply)
	if !ok {
		return reply.NewWrongTypeErrReply()
	}
	// 2. if newKey doesn't exist in the dst node, use rename
	if intReply.Code() == 0 {
		return rename(clusterDatabase, connection, args)
	}

	return intReply
})
