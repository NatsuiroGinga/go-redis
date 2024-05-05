package string

import (
	"math"
	"strconv"
	"testing"
)

const stringOutputPattern = "string values is %s, encoding is %s"
const intOuputPattern = "int values is %d, encoding is %s"

func TestString_SetInt(t *testing.T) {
	s := new(String)
	s.SetInt(10)
	if s.CanInt() {
		t.Logf(intOuputPattern, s.Int(), s.encoding)
	}
	s.SetInt(256 + 10)
	if s.CanInt() {
		t.Logf(intOuputPattern, s.Int(), s.encoding)
	}
	s.SetInt(math.MaxInt16 + 1)
	if s.CanInt() {
		t.Logf(intOuputPattern, s.Int(), s.encoding)
	}
	s.SetInt(math.MaxInt32 + 1)
	if s.CanInt() {
		t.Logf(intOuputPattern, s.Int(), s.encoding)
	}
}

func TestString_SetBytes(t *testing.T) {
	s := new(String)
	s.SetBytes([]byte("hello world"))
	if s.CanString() {
		t.Logf(stringOutputPattern, s.String(), s.encoding)
	}
}

func TestDemo(t *testing.T) {
	i := int8(100)
	s := strconv.Itoa(int(i))
	t.Log(len(s))
}
