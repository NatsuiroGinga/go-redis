package cluster_database

import (
	"context"
	"strconv"
	"strings"
	"sync"

	pool "github.com/jolestar/go-commons-pool/v2"
	"go-redis/client"
	"go-redis/config"
	"go-redis/database"
	"go-redis/datastruct/dict"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/consistenthash"
	"go-redis/lib/id_generator"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// if only one node involved in a transaction, just execute the command don't apply tcc procedure
var allowFastTransaction = true

var router = newRouter()

// ClusterDatabase is a cluster mode database
type ClusterDatabase struct {
	self string // self is the address of the self

	nodes          []string                    // nodes is the address of the peers
	peerPicker     *consistenthash.NodeMap     // peerPicker is the picker of the peers
	peerConnection map[string]*pool.ObjectPool // peerConnection is the connection pool of the peers
	db             db.DBEngine                 // db is the standalone database

	// distributed transaction
	idGenerator  *id_generator.IDGenerator // generate transaction id
	transactions dict.Dict
	txMutex      sync.RWMutex
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
		idGenerator:    id_generator.NewGenerator(config.Properties.Self),
		transactions:   dict.NewNormalDict(),
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

	// Auth
	if cmdName == enum.SYS_AUTH.String() {
		return database.Auth(client, args[1:])
	}
	if !database.IsAuthenticated(client) {
		return &reply.NormalErrReply{Status: "NOAUTH Authentication required"}
	}

	execCmdFunc, ok := router[cmdName]
	if !ok {
		return reply.NewErrReplyByError(enum.NOT_SUPPORTED_CMD)
	}
	result = execCmdFunc(cd, client, args)

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
		cmdName := string(args[0])
		if cmdName == enum.TCC_PREPARE.String() ||
			cmdName == enum.TCC_ROLLBACK.String() ||
			cmdName == enum.TCC_COMMIT.String() {

			return cd.Exec(conn, args)
		}

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

// broadcast 把指令广播给所有节点, 除了发送给本节点请求的节点
func (cd *ClusterDatabase) broadcast(connection resp.Connection, args db.CmdLine) map[string]resp.Reply {
	results := make(map[string]resp.Reply)
	for _, peer := range cd.nodes {
		if peer == cd.self {
			results[peer] = cd.relay(peer, connection, args)
		} else {
			results[peer] = cd.relay(peer, connection, modifyCmd(args, string(args[0])+"_"))
		}
	}
	return results
}
