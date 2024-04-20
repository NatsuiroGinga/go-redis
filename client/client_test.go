package client

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"testing"

	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/parser"
	"go-redis/resp/reply"
)

func TestClient(t *testing.T) {
	client, err := NewClient("127.0.0.1:6379")
	if err != nil {
		t.Error(err)
	}
	client.Start()

	localAddr := client.conn.LocalAddr()
	logger.Info("localAddr:", localAddr)
	listener, err := net.ListenTCP("tcp", localAddr.(*net.TCPAddr))
	if err != nil {
		logger.Error(err)
	}

	conn, err := listener.Accept()
	if err != nil {
		logger.Error(err)
	}
	logger.Info("conn:", conn)
	reader := bufio.NewReader(conn)

	for {
		readBytes, err := reader.ReadBytes('\n')
		// handle the error
		if err != nil {
			if err == io.EOF { // if client closed, close the connection
				logger.Info("client closed")
			} else {
				logger.Warn("read error:", err)
			}

			return
		}
		logger.Info("readBytes:", string(readBytes))
		readBytes = readBytes[:len(readBytes)-1]
		cmd := utils.ToCmdLine3(readBytes)
		r := client.Send(cmd)
		stream := parser.ParseStream(bytes.NewReader(r.Bytes()))
		payload := <-stream
		switch payload.Data.(type) {
		case *reply.MultiBulkReply:
			args := payload.Data.(*reply.MultiBulkReply).Args
			var result []byte
			for _, b := range args {
				for i := range b {
					result = append(result, b[i])
				}
				result = append(result, '\n')
			}
			_, err = conn.Write(result)
		case *reply.BulkReply:
			arg := payload.Data.(*reply.BulkReply).Arg
			_, err = conn.Write(arg)
		}
		if err != nil {
			logger.Error(err)
		}
	}
}

func TestDemo(t *testing.T) {
	client, err := NewClient("localhost:6379")
	if err != nil {
		t.Error(err)
	}
	client.Start()

	result := client.Send([][]byte{
		[]byte("GET"),
		[]byte("class"),
	})
	if re, ok := result.(*reply.NullBulkReply); ok {
		t.Log(string(re.Bytes()))
	} else {
		t.Error("err:", string(result.Bytes()))
	}
}
