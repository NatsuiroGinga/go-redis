package cluster_database

import (
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

var flushdb = cmdFunc(func(clusterDatabase *ClusterDatabase, connection resp.Connection, args db.CmdLine) resp.Reply {
	if len(args) != 1 {
		return reply.NewArgNumErrReply(enum.FLUSHDB.String())
	}

	results := clusterDatabase.broadcast(connection, args)
	for _, r := range results {
		if reply.IsErrReply(r) {
			return r
		}
	}

	return reply.NewOKReply()
})
