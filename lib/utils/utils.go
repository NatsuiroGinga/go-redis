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

// ToCmdLine2 convert commandName and []byte-type argument to CmdLine
func ToCmdLine2(commandName string, args ...[]byte) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = String2Bytes(commandName)
	for i, s := range args {
		result[i+1] = s
	}
	return result
}

// BytesEquals check whether the given bytes is equal
func BytesEquals(a, b []byte) bool {
	return bytes.Compare(a, b) == 0
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
