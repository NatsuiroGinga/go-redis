package main

import (
	"fmt"
	"time"

	"go-redis/config"
	"go-redis/lib/logger"
	"go-redis/resp/handler"
	"go-redis/tcp"
)

func init() {
	logger.Setup(&logger.Settings{
		Path:       "logs",
		Name:       "go-redis",
		Ext:        "log",
		TimeFormat: time.DateOnly,
	})
}

func main() {

	logger.Debug(config.Properties)

	err := tcp.ListenAndServeWithSignal(
		&tcp.Config{
			Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
		},
		handler.NewRespHandler(),
	)

	if err != nil {
		logger.Error("tcp server error:", err)
	}
}
