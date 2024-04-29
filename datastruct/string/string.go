package string

import (
	"reflect"
	"strconv"
	"unsafe"

	"go-redis/lib/utils"
)

type String struct {
	encoding StrType
	content  []byte
	buf      unsafe.Pointer
}

func (str *String) Len() int {
	if str.CanString() {
		return len(str.content)
	}
	if str.CanInt() {
		return int(str.encoding)
	}
	panic("unknown string type")
}

func (str *String) Int() int64 {
	switch str.encoding {
	case INT_8:
		return int64(str.content[0])
	case INT_16:
		return int64(*(*int16)(unsafe.Pointer(&str.content[0])))
	case INT_32:
		return int64(*(*int32)(unsafe.Pointer(&str.content[0])))
	case INT_64:
		return *(*int64)(unsafe.Pointer(&str.content[0]))
	default:
		panic("string can not be converted to int")
	}
}

func (str *String) CanInt() bool {
	switch str.encoding {
	case INT_8, INT_16, INT_32, INT_64:
		return true
	default:
		return false
	}
}

func (str *String) CanString() bool {
	return str.encoding == STRING
}

// String 返回字节数组的深拷贝字符串
func (str *String) String() string {
	return string(str.content)
}

// Bytes 返回字符串的字节切片, 但如果底层存储的是整数, 会把整数转化为字符串再转化为字节切片
func (str *String) Bytes() []byte {
	switch str.encoding {
	case STRING:
		return str.content
	case INT_8, INT_16, INT_32, INT_64:
		intVal := str.Int()
		intStr := strconv.FormatInt(intVal, 10)
		return utils.String2Bytes(intStr)
	default:
		panic("string can not be converted to []byte")
	}
}

// NewString 新建一个String, 根据val的类型自动进行转换
//
// val如果是string或者bytes, 会判断能否转换成int64, 若能转换, 则会存储整数类型的二进制
//
// 如果val不能转换为int64, 会存储字符串转化为的bytes
//
// # 注意: 如果val是字节切片, 那么会使用浅拷贝
//
// 如果val是有符号整数类型, 会存储为对应的整数类型(int8, int16, int32, int64)的二进制
func NewString(val any) *String {
	s := new(String)

	switch data := val.(type) {
	case []byte:
		str := utils.Bytes2String(data)
		intVal, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			s.SetBytes(data)
		} else {
			s.SetInt(intVal)
		}
	case int64:
		s.SetInt(data)
	case int:
		s.SetInt(int64(data))
	case string:
		intVal, err := strconv.ParseInt(data, 10, 64)
		if err != nil {
			s.SetString(data)
		} else {
			s.SetInt(intVal)
		}
	default:
		v := reflect.ValueOf(val)
		if v.CanInt() {
			s.SetInt(v.Int())
		} else {
			panic("value is not int or string or bytes")
		}
	}

	return s
}

// SetInt 根据val的值判断它在(int8, int16, int32, int64)中的哪一个的表示范围, 然后存储为对应类型的二进制
func (str *String) SetInt(val int64) {
	encoding := getIntEncoding(val)
	str.encoding = encoding
	str.content = make([]byte, str.encoding)
	*(*int64)(unsafe.Pointer(&str.content[0])) = val
}

// SetBytes 以浅拷贝方式直接设置字节切片
func (str *String) SetBytes(val []byte) {
	str.content = val
	str.encoding = STRING
}

// SetString 直接把字符串转化为字节切片
func (str *String) SetString(val string) {
	str.content = utils.String2Bytes(val)
	str.encoding = STRING
}

func getIntEncoding(val int64) StrType {
	encoding := INT_8
	if val > 0x7F || val < -0x80 {
		encoding = INT_16
	}
	if val > 0x7FFF || val < -0x8000 {
		encoding = INT_32
	}
	if val > 0x7FFFFFFF || val < -0x80000000 {
		encoding = INT_64
	}
	return encoding
}
