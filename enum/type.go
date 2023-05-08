package enum

import "fmt"

var types = [...]string{
	"string",
	"list",
}

// DataType 数据类型
type DataType int

const (
	String DataType = iota // 字符串
	List
)

// DataType 与Stringer接口区别
func (dataType DataType) DataType() {
}

func (dataType DataType) String() string {
	if 0 <= dataType && int(dataType) < len(types) {
		return types[dataType]
	}
	return fmt.Sprintf("type %d", dataType)
}
