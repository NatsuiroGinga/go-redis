package cluster_database

import (
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
)

func execSelect(clusterDatabase *ClusterDatabase, connection resp.Connection, args db.CmdLine) resp.Reply {
	return clusterDatabase.relay(clusterDatabase.self, connection, args)
}

func init() {
	registerRouter(enum.SELECT, execSelect)
}
