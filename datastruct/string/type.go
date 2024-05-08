package string

// StrType 是String编码的枚举类
type StrType int8

func (sType StrType) String() string {
	switch sType {
	case 1:
		return "int8"
	case 2:
		return "int16"
	case 4:
		return "int32"
	case 8:
		return "int64"
	case -1:
		return "string"
	default:
		return "unknown encoding"
	}
}

const (
	INT_8  StrType = 1 << iota // int8
	INT_16                     // int16
	INT_32                     // int32
	INT_64                     // int64
	STRING StrType = -1        // string
)
