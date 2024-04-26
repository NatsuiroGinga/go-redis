package cluster_database

import (
	"strconv"

	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

// execDel Del atomically removes given writeKeys from cluster, writeKeys can be distributed on any node
// if the given writeKeys are distributed on different node, Del will use try-commit-catch to remove them
func execDel(clusterDatabase *ClusterDatabase, connection resp.Connection, args db.CmdLine) resp.Reply {
	if len(args) == 0 {
		return reply.NewUnknownErrReply()
	}
	if len(args) == 1 {
		return reply.NewArgNumErrReply(enum.DEL.String())
	}
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i])
	}
	groupMap := clusterDatabase.groupBy(keys)
	if len(groupMap) == 1 { // do fast
		for peer := range groupMap { // only one peerKey
			return clusterDatabase.relay(peer, connection, args)
		}
	}
	// prepare
	var errReply resp.Reply
	txID := clusterDatabase.idGenerator.NextID()
	txIDStr := strconv.FormatInt(txID, 10)
	rollback := false
	for peer, peerKeys := range groupMap {
		peerArgs := []string{txIDStr, enum.DEL.String()}
		peerArgs = append(peerArgs, peerKeys...)
		var resp resp.Reply
		resp = clusterDatabase.relay(peer, connection, makeArgs(enum.TCC_PREPARE.String(), peerArgs...))
		if reply.IsErrReply(resp) {
			errReply = resp
			rollback = true
			break
		}
	}
	var respList []resp.Reply
	if rollback {
		// rollback
		requestRollback(clusterDatabase, connection, txID, groupMap)
	} else {
		// commit
		respList, errReply = requestCommit(clusterDatabase, connection, txID, groupMap)
		if errReply != nil {
			rollback = true
		}
	}
	if !rollback {
		var deleted int64 = 0
		for _, resp := range respList {
			intResp := resp.(*reply.IntReply)
			deleted += intResp.Code()
		}
		return reply.NewIntReply(deleted)
	}
	return errReply
}

func init() {
	registerRouter(enum.DEL, execDel)
}
