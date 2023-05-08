package cluster_database

import (
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

var rename = CmdFunc(func(clusterDatabase *ClusterDatabase, connection resp.Connection, args db.CmdLine) resp.Reply {
	if len(args) != 3 {
		return reply.NewArgNumErrReply(enum.RENAME.String())
	}
	oldKey := utils.Bytes2String(args[1])
	newKey := utils.Bytes2String(args[2])
	src := clusterDatabase.peerPicker.Pick(oldKey)
	dst := clusterDatabase.peerPicker.Pick(newKey)

	return utils.If(src == dst, // if the src and dst are in the same node, just relay the command
		clusterDatabase.relay(src, connection, args),                           // relay the command
		renameCrossSlot(clusterDatabase, connection, oldKey, newKey, src, dst)) // rename the key from one slot to another
})

// renameCrossSlot is the function for rename command, it will rename the key from one slot to another
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
