package cluster_database

import (
	"go-redis/interface/db"
	"go-redis/interface/resp"
)

var ping = cmdFunc(func(clusterDatabase *ClusterDatabase, connection resp.Connection, args db.CmdLine) resp.Reply {
	return clusterDatabase.relay(clusterDatabase.self, connection, args)
})
