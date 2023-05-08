package utils

import (
	"log"
	"testing"
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
