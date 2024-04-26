package aof

import (
	"io"
	"os"
	"strconv"

	"go-redis/config"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/connection"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
)

const ChanSize = 1 << 16

type Handler struct {
	database    db.Database   // 数据库
	aofFile     *os.File      // aof文件
	aofFilename string        // aof文件名
	currentDB   int           // 当前数据库
	aofChan     chan *payload // aof文件通道缓冲区
}

// NewHandler 创建一个aof处理器
func NewHandler(database db.Database) (*Handler, error) {
	file, err := os.OpenFile(config.Properties.AppendFilename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler := &Handler{
		database:    database,
		aofFilename: config.Properties.AppendFilename,
		aofFile:     file,
		aofChan:     make(chan *payload, ChanSize),
	}
	handler.Load()    // 从aof文件加载数据到内存
	go handler.Save() // 启动一个守护协程, 将aof文件保存到磁盘

	return handler, nil
}

// Append 添加一个命令到aof文件
func (handler *Handler) Append(dbIndex int, cmd db.CmdLine) {
	if !config.Properties.AppendOnly || handler.aofChan == nil {
		return
	}

	handler.aofChan <- newPayload(cmd, dbIndex)
}

// Save 将aof文件保存到磁盘
func (handler *Handler) Save() {
	handler.currentDB = 0

	for p := range handler.aofChan {
		// 切换数据库
		if p.dbIndex != handler.currentDB {
			cmd := utils.ToCmdLine(enum.SELECT.String(), strconv.Itoa(p.dbIndex))
			multiBulkReply := reply.NewMultiBulkReply(cmd)
			_, err := handler.aofFile.Write(multiBulkReply.Bytes())
			if err != nil {
				logger.Error(err)
				continue
			}
			handler.currentDB = p.dbIndex
		}

		b := reply.NewMultiBulkReply(p.cmdLine).Bytes()

		_, err := handler.aofFile.Write(b)
		if err != nil {
			logger.Error(err)
		}
	}
}

// Load 从aof文件加载数据
func (handler *Handler) Load() {
	file, err := os.Open(handler.aofFilename)
	if err != nil {
		logger.Error(err)
		return
	}
	defer file.Close()

	ch := parser.ParseStream(file)
	fakeConnection := &connection.RespConnection{}
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error(p.Err)
			continue
		}

		if p.Data == nil {
			logger.Error("aof load data is nil")
			continue
		}

		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("aof load data type error")
		}
		result := handler.database.Exec(fakeConnection, r.Args)
		if reply.IsErrReply(result) {
			logger.Error(result.(resp.ErrorReply))
		}
	}
}

type payload struct {
	cmdLine db.CmdLine
	dbIndex int
}

func newPayload(cmdLine db.CmdLine, dbIndex int) *payload {
	return &payload{cmdLine: cmdLine, dbIndex: dbIndex}
}
