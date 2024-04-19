package client

import (
	"errors"
	"net"
	"runtime/debug"
	"sync"
	"time"

	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/lib/sync/wait"
	"go-redis/lib/utils"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
)

// Client is a pipeline mode redis client
type Client struct {
	conn        net.Conn
	pendingReqs chan *request // wait to send
	waitingReqs chan *request // waiting response
	ticker      *time.Ticker
	addr        string

	working *sync.WaitGroup // its counter presents unfinished requests(pending and waiting)
}

// request is a message sends to redis server
type request struct {
	id        uint64
	args      [][]byte
	reply     resp.Reply
	heartbeat bool
	waiting   *wait.Wait
	err       error
}

const (
	chanSize = 1 << 8
	maxWait  = 3 * time.Second
	network  = "tcp"
)

// NewClient creates a new client
func NewClient(addr string) (client *Client, err error) {
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		addr:        addr,
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		working:     &sync.WaitGroup{},
	}, nil
}

// Start starts asynchronous goroutines
func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	go client.handleWrite()
	go func() {
		err := client.handleRead()
		if err != nil {
			logger.Error(err)
		}
	}()
	go client.heartbeat()
}

// Close stops asynchronous goroutines and close connection
func (client *Client) Close() {
	client.ticker.Stop()
	// stop new request
	close(client.pendingReqs)

	// wait stop process
	client.working.Wait()

	// clean
	_ = client.conn.Close()
	close(client.waitingReqs)
}

func (client *Client) handleConnectionError() error {
	err1 := client.conn.Close()
	if err1 != nil {
		var opErr *net.OpError
		if errors.As(err1, &opErr) {
			if opErr.Err.Error() != enum.CONNECTION_CLOSED.Error() {
				return err1
			}
		}
	}
	conn, err1 := net.Dial(network, client.addr)
	if err1 != nil {
		logger.Error(err1)
		return err1
	}
	client.conn = conn
	go client.handleRead()

	return nil
}

func (client *Client) heartbeat() {
	for range client.ticker.C {
		client.doHeartbeat()
	}
}

func (client *Client) handleWrite() {
	for req := range client.pendingReqs {
		client.doRequest(req)
	}
}

// Send sends a request to redis server
func (client *Client) Send(args db.CmdLine) resp.Reply {
	req := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- req
	timeout := req.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return reply.NewErrReply(enum.SERVER_TIMEOUT.Error())
	}

	return utils.If(req.err == nil, req.reply, reply.NewErrReply(enum.REQUEST_FAILED.Error()))
}

func (client *Client) doHeartbeat() {
	req := &request{
		args:      [][]byte{utils.String2Bytes(enum.PING.String())},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)
	client.working.Add(1)
	defer client.working.Done()
	client.pendingReqs <- req
	req.waiting.WaitWithTimeout(maxWait)
}

func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	re := utils.If2Kinds(len(req.args) == 1,
		reply.NewBulkReply(req.args[0]),
		reply.NewMultiBulkReply(req.args)).(resp.Reply)
	bytes := re.Bytes()
	_, err := client.conn.Write(bytes)
	i := 0
	for err != nil && i < 3 {
		err = client.handleConnectionError()
		if err == nil {
			_, err = client.conn.Write(bytes)
		}
		i++
	}
	if err == nil {
		client.waitingReqs <- req
		return
	}

	req.err = err
	req.waiting.Done()
}

func (client *Client) finishRequest(reply resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()

	req := <-client.waitingReqs
	if req == nil {
		return
	}
	req.reply = reply
	if req.waiting != nil {
		req.waiting.Done()
	}
}

func (client *Client) handleRead() error {
	ch := parser.ParseStream(client.conn)
	for payload := range ch {
		if payload.Err != nil {
			client.finishRequest(reply.NewErrReply(payload.Err.Error()))
			continue
		}
		client.finishRequest(payload.Data)
	}

	return nil
}
