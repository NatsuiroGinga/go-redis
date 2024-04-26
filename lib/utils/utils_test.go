package utils

import (
	"testing"
	"time"
)

func TestToCmdLine3(t *testing.T) {
	timestamp := time.Unix(1713597327, 0)
	t.Log(timestamp.Format("2006-01-02 15:04:05"))
}

func TestClose(t *testing.T) {

}
