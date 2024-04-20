package database

import (
	"testing"
	"time"

	"go-redis/lib/logger"
	"go-redis/lib/utils"
)

func TestExpire(t *testing.T) {
	d := newDB(0)
	execSet(d, utils.ToCmdLine("name", "jack"))
	execExpire(d, utils.ToCmdLine("name", "5"))
	select {
	case <-time.After(time.Second * 10):
		logger.Info("exit")
		// case <-time.After(3 * time.Second):
		// 	d.Persist("expire-name")
	}
}
