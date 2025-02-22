package config

import (
	"testing"
	"time"
)

func TestDemo(t *testing.T) {
	milli := time.Now().Add(100 * time.Second).UnixMilli()
	t.Log(milli)
}
