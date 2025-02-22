package string

import (
	"reflect"
	"strconv"
	"unsafe"

	"go-redis/lib/utils"
)

// String 实现String基础类型
type String struct {
	encoding StrType // 类型枚举, 包括STRING、INT_8、INT_16、INT_32和INT_64
	content  []byte  // 存储字节数组
}

func (str *String) Append(val []byte) {
	str.content = append(str.content, val...)
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

// Int 将String实例转化为int64的整数
func (str *String) Int() int64 {
	switch str.encoding { // 根据String实例的编码实现不同的逻辑
	case INT_8: // int8直接转化即可
		return int64(str.content[0])
	case INT_16: // int16需要取字节数组头两个字节转化
		return int64(*(*int16)(unsafe.Pointer(&str.content[0])))
	case INT_32: // int32需要取字节数组的头4个字节转化
		return int64(*(*int32)(unsafe.Pointer(&str.content[0])))
	case INT_64: // int64要取字节数组的头8个字节转化
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
	return string(str.Bytes())
}

// Bytes 返回字符串的字节切片(浅拷贝), 但如果底层存储的是整数, 会把整数转化为字符串再转化为字节切片
func (str *String) Bytes() []byte {
	if str == nil {
		return nil
	}
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

// SetInt 根据val的值判断它在(int8, int16, int32, int64)中的哪一个的表示范围, 然后存储对应类型的二进制
func (str *String) SetInt(val int64) {
	// 1. 获取val的编码
	encoding := getIntEncoding(val)
	// 2. 设置实例的编码
	str.encoding = encoding
	// 3. 存储val的二进制
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

// getIntEncoding 获取val的实际编码, 可能为int8, int16, int32, int64
func getIntEncoding(val int64) StrType {
	// 1. 初始化为int8
	encoding := INT_8
	// 2. 判定是否处于int16的范围内
	if val > 0x7F || val < -0x80 {
		encoding = INT_16
	}
	// 3. 判定是否处于int32的范围内
	if val > 0x7FFF || val < -0x8000 {
		encoding = INT_32
	}
	// 4. 判定是否处于int64的范围内
	if val > 0x7FFFFFFF || val < -0x80000000 {
		encoding = INT_64
	}
	return encoding
}
