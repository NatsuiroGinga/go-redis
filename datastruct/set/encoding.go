package set

type intSetEncoding int32

const (
	INT_16 intSetEncoding = 1 << (iota + 1)
	INT_32
	INT_64
)
