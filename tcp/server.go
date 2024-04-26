package tcp

import (
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go-redis/interface/tcp"
	"go-redis/lib/logger"
)

// Config defines the configuration for the tcp server.
// Address is the TCP address to listen on, ":6379" if empty.
type Config struct {
	Address string
}

// ListenAndServeWithSignal listens on the TCP network address addr and then
// calls Serve to handle requests on incoming connections.
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	// listen for the close signal.
	sigChan := make(chan os.Signal)
	// register the close signal.
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		// wait for the close signal.
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			go logger.Info("receive signal", sig)
			// close the listener and handler.
			closeChan <- struct{}{}
		}
	}()

	// listen for an incoming connection.
	listen, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}

	go logger.Info("tcp server start listening on", cfg.Address)
	// handle the connection in a new goroutine.
	err = listenAndServe(listen, handler, closeChan)

	if err != nil {
		return err
	}

	return nil
}

// listenAndServe listens on the TCP network address addr and then
// calls Serve to handle requests on incoming connections.
func listenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) error {
	go func() {
		// wait for the close signal.
		<-closeChan
		// close the listener when the application closes.
		go logger.Info("tcp server stop listening on", listener.Addr())
		// close the listener and handler.
		release(listener, handler)
	}()

	defer release(listener, handler)

	ctx := context.Background()
	wg := sync.WaitGroup{}

	for {
		// listen for an incoming connection.
		conn, err := listener.Accept()
		if err != nil {
			logger.Error(err)
			break
		}

		go logger.Info("accept a new connection:", conn.RemoteAddr())

		// handle the connection in a new goroutine.
		wg.Add(1)

		go func() {
			defer wg.Done()
			handler.Handle(ctx, conn)
		}()
	}

	// wait for all goroutines to complete.
	wg.Wait()

	return nil
}

// release the listener and handler.
func release(listener net.Listener, handler tcp.Handler) {
	listener.Close()
	handler.Close()
}
