package utils

import (
	"log"
	"testing"

	"go-redis/lib/logger"
)

func TestString2Bytes(t *testing.T) {
	s := "hello"
	log.Println("s", s)
	b := String2Bytes(s)
	log.Println("b", string(b))
	if !BytesEquals(b, []byte(s)) {
		t.Errorf("String2Bytes failed")
	}
}

func TestBytes2String(t *testing.T) {
	b := []byte("123j")
	log.Println("cap", cap(b))
	s := Bytes2String(b)
	log.Println(s)
}

func TestCopySlices(t *testing.T) {
	src := [][]byte{[]byte("123"), []byte("abc"), []byte("+-/")}
	dst := CopySlices(src[:1])
	logger.Info(dst)
}
