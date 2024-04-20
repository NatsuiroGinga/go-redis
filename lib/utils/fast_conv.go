package utils

import (
	"unsafe"
)

// Bytes2String convert bytes to string, only readable
//
// NOTE: this function is not safe, it may cause memory leak
func Bytes2String(b []byte) (s string) {
	return *(*string)(unsafe.Pointer(&b))
}

// String2Bytes convert string to bytes, only readable
//
// NOTE: this function is not safe, it may cause memory leak
func String2Bytes(s string) (b []byte) {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// CopySlices copy src to dst, only readable
//
// NOTE: this function is not safe, it may cause memory leak
func CopySlices(src [][]byte) (dst []string) {
	dst = make([]string, 0, len(src))
	for i := range src {
		dst = append(dst, Bytes2String(src[i]))
	}
	return
}
