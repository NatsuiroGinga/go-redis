package cluster_database

import (
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/resp/reply"
)

var del = CmdFunc(func(clusterDatabase *ClusterDatabase, connection resp.Connection, args db.CmdLine) resp.Reply {
	if len(args) == 0 {
		return reply.NewUnknownErrReply()
	}
	if len(args) == 1 {
		return reply.NewArgNumErrReply(enum.DEL.String())
	}
	n := int64(0)
	results := clusterDatabase.broadcast(connection, args)
	for _, r := range results {
		if reply.IsErrReply(r) {
			return r
		}

		intReply, ok := r.(*reply.IntReply)

		if !ok {
			return reply.NewWrongTypeErrReply()
		}
		n += intReply.Code()
	}

	return reply.NewIntReply(n)
})
