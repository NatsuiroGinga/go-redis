package handler

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	cluster_database "go-redis/cluster"
	"go-redis/config"
	database2 "go-redis/database"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
)

// RespHandler is the handler of RESP protocol.
type RespHandler struct {
	activeConn sync.Map
	db         db.Database
	closing    atomic.Bool
}

func NewRespHandler() (handler *RespHandler) {
	handler = new(RespHandler)

	if config.Properties.Self != "" && len(config.Properties.Peers) > 0 {
		handler.db = cluster_database.NewClusterDatabase()
	} else {
		handler.db = database2.NewStandaloneDatabase()
	}

	return
}

func (rh *RespHandler) closeClient(client *connection.RespConnection) {
	_ = client.Close()
	rh.db.AfterClientClose(client)
	rh.activeConn.Delete(client)
}

// Handle handles the connection.
func (rh *RespHandler) Handle(_ context.Context, conn net.Conn) {
	if rh.closing.Load() { // if handler is closing, close the connection
		_ = conn.Close()
		return
	}
	// create a new client
	client := connection.NewRespConnection(conn)
	rh.activeConn.Store(client, struct{}{})
	ch := parser.ParseStream(conn)
	// receive payload
	for payload := range ch {
		if payload.Err != nil {
			// if client closed, close the connection
			if payload.Err == io.EOF ||
				errors.Is(payload.Err, io.ErrUnexpectedEOF) ||
				strings.Contains(payload.Err.Error(), enum.CONNECTION_CLOSED.Error()) {

				rh.closeClient(client)
				go logger.Info("client closed:", client.RemoteAddr())
				return
			}
			// protocol error
			_, err := client.Write([]byte(payload.Err.Error()))

			if err != nil {
				rh.closeClient(client)
				go logger.Info("client closed:", client.RemoteAddr())
				return
			}

			continue
		}

		if payload.Data == nil {
			go logger.Info(enum.EMPTY_PAYLOAD)
			continue
		}

		result := rh.exec(payload, client)

		if result != nil {
			_, _ = client.Write(result.Bytes())
		} else {
			_, _ = client.Write(reply.NewUnknownErrReply().Bytes())
		}
	}
}

// exec 使用数据库根据解析后的客户端的回复执行命令, 然后返回结果
func (rh *RespHandler) exec(payload *parser.Payload, client *connection.RespConnection) resp.Reply {
	switch payload.Data.(type) {
	case *reply.MultiBulkReply:
		return rh.db.Exec(client, payload.Data.(*reply.MultiBulkReply).Args)
	case *reply.BulkReply:
		return rh.db.Exec(client, [][]byte{payload.Data.(*reply.BulkReply).Arg})
	default: // 错误回复
		return payload.Data
	}
}

// Close closes the handler.
func (rh *RespHandler) Close() error {
	go logger.Info("RespHandler closing...")

	rh.closing.Store(true)
	rh.activeConn.Range(func(key, value any) bool {
		client := key.(*connection.RespConnection)
		_ = client.Close()
		return true
	})
	_ = rh.db.Close()
	return nil
}
