package cluster_database

import (
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// CmdFunc is the function type for commands
type CmdFunc func(clusterDatabase *ClusterDatabase, connection resp.Connection, args db.CmdLine) resp.Reply

// newRouter returns a router map for commands
func newRouter() (routerMap map[string]CmdFunc) {
	return map[string]CmdFunc{
		enum.EXISTS.String():   defaultFunc,
		enum.TYPE.String():     defaultFunc,
		enum.GET.String():      defaultFunc,
		enum.KEYS.String():     defaultFunc,
		enum.SET.String():      defaultFunc,
		enum.GETSET.String():   defaultFunc,
		enum.SETNX.String():    defaultFunc,
		enum.PING.String():     ping,
		enum.RENAME.String():   rename,
		enum.RENAMENX.String(): renamenx,
		enum.FLUSHDB.String():  flushdb,
		enum.DEL.String():      del,
		enum.SELECT.String():   execSelect,
	}
}

// defaultFunc is the default function for commands, it will relay the command to the peer
//
// exp.
//
//	> SET key value
//	< OK
var defaultFunc = CmdFunc(func(clusterDatabase *ClusterDatabase, connection resp.Connection, args db.CmdLine) resp.Reply {
	if len(args) == 0 {
		return reply.NewUnknownErrReply()
	}
	if len(args) == 1 {
		return reply.NewArgNumErrReply(utils.Bytes2String(args[0]))
	}
	key := utils.Bytes2String(args[1])
	peer := clusterDatabase.peerPicker.Pick(key)

	return clusterDatabase.relay(peer, connection, args)
})
