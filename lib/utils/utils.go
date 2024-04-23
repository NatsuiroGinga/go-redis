package utils

import (
	"bytes"
)

// ToCmdLine convert strings to [][]byte
func ToCmdLine(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, s := range cmd {
		args[i] = String2Bytes(s)
	}
	return args
}

// ToCmdLine2 convert commandName and []byte-type arguments to CmdLine
func ToCmdLine2(commandName string, args ...[]byte) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = String2Bytes(commandName)
	copy(result[1:], args)
	return result
}

func ToCmdLine3(cmd []byte) [][]byte {
	if len(cmd) > 0 && cmd[len(cmd)-1] == '\n' {
		cmd = cmd[:len(cmd)-1]
	}
	// trim front and back space
	cmd = bytes.TrimSpace(cmd)
	// split bytes
	params := bytes.Split(cmd, String2Bytes(" "))
	result := make([][]byte, 0, len(params))
	for _, param := range params {
		if len(param) > 0 { // delete empty bytes
			result = append(result, param)
		}
	}
	return result
}

// BytesEquals check whether the given bytes is equal
func BytesEquals(a, b []byte) bool {
	return bytes.Equal(a, b)
}

// Equals check whether the given value is equal
func Equals(a, b any) bool {
	sliceA, okA := a.([]byte)
	sliceB, okB := b.([]byte)
	if okA && okB {
		return BytesEquals(sliceA, sliceB)
	}
	return a == b
}

// If returns trueVal if condition is true, otherwise falseVal.
func If[T any](condition bool, trueVal, falseVal T) T {
	if condition {
		return trueVal
	}
	return falseVal
}

// If2Kinds returns trueVal if condition is true, otherwise falseVal.
//
// This function is used to avoid the type of trueVal and falseVal is not the same.
func If2Kinds(condition bool, trueVal, falseVal any) any {
	if condition {
		return trueVal
	}
	return falseVal
}

func Assert(condition bool) {
	if !condition {
		panic("assertion failed")
	}
}
