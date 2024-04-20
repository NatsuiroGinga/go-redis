package dict

import (
	"strconv"
	"testing"
)

func TestToLockIndices(t *testing.T) {
	d := NewConcurrentDict(0)
	size := 100
	for i := 0; i < size; i++ {
		key := "k" + strconv.Itoa(i)
		d.Set(key, i)
	}
	indices := d.toLockIndices([]string{"k1", "k2", "k100", "k999"}, false)
	t.Log(indices)
}
