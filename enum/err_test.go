package enum

import (
	"testing"
)

func TestDemo(t *testing.T) {
	i := float64(1<<32 - 1)
	u := uint32(i)
	t.Log(u)
}
