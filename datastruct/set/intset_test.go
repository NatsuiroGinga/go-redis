package set

import (
	"math"
	"testing"
)

func TestAdd(t *testing.T) {
	intSet := NewIntSet()
	intSet.Add(1)
	intSet.Add(266)
	// intSet.Add(1)
	intSet.Add(math.MaxInt16 + 1)
	// intSet.Add(math.MaxInt32 + 100)
	// intSet.Add(math.MaxInt32 + 1)
	intSet.print()
}
