package client

import (
	"errors"
	"net"
	"runtime/debug"
	"strings"
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

// Client 是一个Pipeline模式的客户端
type Client struct {
	conn        net.Conn      // 与服务端的链接
	pendingReqs chan *request // 等待发送的请求队列
	waitingReqs chan *request // 等待服务器响应的请求队列
	ticker      *time.Ticker  // 发送心跳的计时器
	addr        string        // 服务器地址

	working *sync.WaitGroup // 统计未完成的任务, 包括未发送和未响应的请求
}

// request 是发送给服务端的请求信息
type request struct {
	id        uint64     // 请求id
	args      [][]byte   // 请求参数
	reply     resp.Reply // 请求回复
	heartbeat bool       // 标记是否为心跳请求
	waiting   *wait.Wait // 调用协程发送请求后通过 waitgroup 等待请求异步处理完成
	err       error      // 错误信息
}

const (
	chanSize  = 1 << 8           // 请求队列的容量
	maxWait   = 3 * time.Second  // 最大的等待时间
	network   = "tcp"            // 网络链接方式
	heartbeat = 10 * time.Second // 发送心跳的间隔
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
		working:     new(sync.WaitGroup),
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
	go func() {
		err := client.handleRead()
		if err != nil {
			logger.Error(err)
		}
	}()

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

	if req.err != nil {
		return reply.NewErrReply(enum.REQUEST_FAILED.Error())
	}

	return req.reply
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
	var err error
	for i := 0; i < 3; i++ { // only retry, waiting for handleRead
		_, err = client.conn.Write(bytes)
		if err == nil ||
			(!strings.Contains(err.Error(), "timeout") && // only retry timeout
				!strings.Contains(err.Error(), "deadline exceeded")) {
			break
		}
		logger.Error(err)
	}
	if err == nil {
		client.waitingReqs <- req
		return
	}
	logger.Error(err)
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
