package enum

import "fmt"

var types = [...]string{
	1: "string",
	2: "list",
	3: "zset",
	4: "hash",
	5: "set",
}

// DataType 数据类型
type DataType int

const (
	TYPE_STRING DataType = iota + 1 // 字符串
	TYPE_LIST                       // 存储任意类型的列表
	TYPE_ZSET                       // 存储string-float的有序集合
	TYPE_HASH                       // 存储string-any的散列
	TYPE_SET                        // string的无序集合
)

// DataType 与Stringer接口区分
func (dataType DataType) DataType() {
}

func (dataType DataType) String() string {
	if 0 < dataType && int(dataType) < len(types) {
		return types[dataType]
	}
	return fmt.Sprintf("type %d", dataType)
}
