package utils

import (
	"unsafe"
)

// Bytes2String convert bytes to string
//
// NOTE: this function is not safe, it may cause memory leak
func Bytes2String(b []byte) (s string) {
	return *(*string)(unsafe.Pointer(&b))
}

// String2Bytes convert string to bytes
//
// NOTE: this function is not safe, it may cause memory leak
func String2Bytes(s string) (b []byte) {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
