package set

import (
	"math"
	"testing"
)

func TestAdd(t *testing.T) {
	intSet := NewIntSet()
	intSet.Add(int64(100))
	intSet.Add(int64(math.MaxInt16 + 1))
	intSet.Add(int64(math.MaxInt32 + 1))
}
