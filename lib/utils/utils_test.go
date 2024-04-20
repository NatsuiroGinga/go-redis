package utils

import "testing"

func TestToCmdLine3(t *testing.T) {
	s := []byte("set name jack")
	cmd := ToCmdLine3(s)
	for _, b := range cmd {
		t.Log(string(b))
	}
}
