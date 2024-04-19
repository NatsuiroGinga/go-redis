package list

import (
	"testing"

	"go-redis/config"
)

func TestPushBack(t *testing.T) {
	quickList := NewQuickList()
	for i := 0; i < config.Properties.ListMaxShardSize+1; i++ {
		quickList.PushBack(i)
	}
}
