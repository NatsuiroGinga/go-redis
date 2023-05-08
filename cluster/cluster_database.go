package cluster_database

import (
	"context"
	pool "github.com/jolestar/go-commons-pool/v2"
	"go-redis/config"
	"go-redis/database"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/consistenthash"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/client"
	"go-redis/resp/reply"
	"strconv"
	"strings"
)

var router = newRouter()

// ClusterDatabase is a cluster mode database
type ClusterDatabase struct {
	self string // self is the address of the self

	nodes          []string                    // nodes is the address of the peers
	peerPicker     *consistenthash.NodeMap     // peerPicker is the picker of the peers
	peerConnection map[string]*pool.ObjectPool // peerConnection is the connection pool of the peers
	db             db.Database                 // db is the standalone database
}

func NewClusterDatabase() *ClusterDatabase {
	peerConnection := make(map[string]*pool.ObjectPool)
	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	nodes = append(append(nodes, config.Properties.Peers...), config.Properties.Self)
	ctx := context.Background()

	for _, peer := range config.Properties.Peers {
		peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, newConnectionFactory(peer))
	}

	return &ClusterDatabase{
		self:           config.Properties.Self,
		nodes:          nodes,
		peerPicker:     consistenthash.NewNodeMap(nil).Add(nodes...),
		peerConnection: peerConnection,
		db:             database.NewStandaloneDatabase(),
	}
}

func (cd *ClusterDatabase) Exec(client resp.Connection, args db.CmdLine) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
			result = reply.NewUnknownErrReply()
		}
	}()

	if len(args) == 0 {
		return reply.NewUnknownErrReply()
	}
	cmdName := strings.ToUpper(utils.Bytes2String(args[0]))
	cmdFunc, ok := router[cmdName]
	if !ok {
		return reply.NewErrReplyByError(enum.NOT_SUPPORTED_CMD)
	}
	result = cmdFunc(cd, client, args)

	return
}

func (cd *ClusterDatabase) Close() error {
	return cd.db.Close()
}

func (cd *ClusterDatabase) AfterClientClose(client resp.Connection) {
	cd.db.AfterClientClose(client)
}

// getPeerClient get a client from the pool
func (cd *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	connectionPool, ok := cd.peerConnection[peer]
	if !ok {
		return nil, PEER_NOT_FOUND
	}
	object, err := connectionPool.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}

	c, ok := object.(*client.Client)
	if !ok {
		return nil, TYPE_MISMATCH
	}

	return c, nil
}

// returnPeerClient returns the client to the pool
func (cd *ClusterDatabase) returnPeerClient(peer string, oneClient *client.Client) error {
	objectPool, ok := cd.peerConnection[peer]
	if !ok {
		return PEER_NOT_FOUND
	}

	return objectPool.ReturnObject(context.Background(), oneClient)
}

// relay relays the command to the peer
func (cd *ClusterDatabase) relay(peer string, conn resp.Connection, args db.CmdLine) resp.Reply {
	if peer == cd.self {
		return cd.db.Exec(conn, args)
	}

	oneClient, err := cd.getPeerClient(peer)
	if err != nil {
		return reply.NewErrReply(err.Error())
	}
	defer cd.returnPeerClient(peer, oneClient)

	// 选择DB
	dbIndex := conn.GetDBIndex()
	if dbIndex != 0 {
		cmdLine := utils.ToCmdLine(enum.SELECT.String(), strconv.Itoa(dbIndex))
		oneClient.Send(cmdLine)
	}

	return oneClient.Send(args)
}

// broadcast broadcasts the command to all peers
func (cd *ClusterDatabase) broadcast(connection resp.Connection, args db.CmdLine) (results map[string]resp.Reply) {
	results = make(map[string]resp.Reply)
	for _, peer := range cd.nodes {
		results[peer] = cd.relay(peer, connection, args)
	}

	return results
}
