package set

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"sort"
	"time"
	"unsafe"
)

// IntSet 是一个整数集合，可以存储 int16, int32 或 int64 类型的整数
type IntSet struct {
	encoding int32  // 表示当前存储的整数类型
	length   int32  // 集合中元素的数量
	contents []byte // 存储元素的切片
}

// transVal2Int64 把整型数值转化为int64
func transVal2Int64(val any) (int64, error) {
	value := reflect.ValueOf(val)
	if value.CanInt() {
		return value.Int(), nil
	}
	return 0, errors.New("val is not int type")
}

// Add 接收int64类型的整数
func (set *IntSet) Add(val any) int {
	value, err := transVal2Int64(val)
	if err != nil {
		return 0
	}
	// 1. 确定新值的编码类型
	newEncoding := getValEncoding(value)

	// 2. 如果需要，升级现有元素
	if newEncoding > set.encoding {
		newContents := make([]byte, set.Len()*int(newEncoding))
		for i := int32(0); i < int32(set.Len()); i++ {
			*(*int64)(unsafe.Pointer(&newContents[i*newEncoding])) = set.get(int(i))
		}
		set.contents = newContents
		set.encoding = newEncoding
	}
	// 3. 找到元素所在起始位置
	found, pos := set.intsetSearch(value)
	if found { // 3.1 元素已经存在, 不做插入, 返回
		return 0
	}

	// 4. 添加新元素
	set.contents = slices.Insert(set.contents, pos*int(newEncoding), make([]byte, newEncoding)...)
	switch newEncoding {
	case 2:
		*(*int16)(unsafe.Pointer(&set.contents[pos*2])) = int16(value)
	case 4:
		*(*int32)(unsafe.Pointer(&set.contents[pos*4])) = int32(value)
	case 8:
		*(*int64)(unsafe.Pointer(&set.contents[pos*8])) = value
	}
	// 5. 集合元素数量加1
	set.length++

	return 1
}

// intsetSearch 在 IntSet 中查找给定的整数值, 返回值在字节数组中的相对位置
//
// 值在字节数组中的真实起始下标应该使用 pos * encoding
//
// 如果找到了值, 返回相对位置; 否则返回可以插入值的相对位置
func (set *IntSet) intsetSearch(value int64) (found bool, pos int) {
	pos = sort.Search(set.Len(), func(i int) bool {
		return set.get(i) >= value
	})

	return pos < set.Len() && set.get(pos) == value, pos // 没找到值，返回应该插入的位置
}

// get 从 IntSet 中获取指定位置的整数值
func (set *IntSet) get(pos int) int64 {
	switch set.encoding {
	case 2:
		return int64(int16(set.contents[pos*2]) | int16(set.contents[pos*2+1])<<8)
	case 4:
		return int64(int32(set.contents[pos*4]) |
			int32(set.contents[pos*4+1])<<8 |
			int32(set.contents[pos*4+2])<<16 |
			int32(set.contents[pos*4+3])<<24)
	case 8:
		return int64(set.contents[pos*8]) |
			int64(set.contents[pos*8+1])<<8 |
			int64(set.contents[pos*8+2])<<16 |
			int64(set.contents[pos*8+3])<<24 |
			int64(set.contents[pos*8+4])<<32 |
			int64(set.contents[pos*8+5])<<40 |
			int64(set.contents[pos*8+6])<<48 |
			int64(set.contents[pos*8+7])<<56
	default:
		panic("unsupported encoding")
	}
}

// getValEncoding 获取元素的编码
func getValEncoding(val int64) int32 {
	encoding := int32(2)
	if val > 0x7FFF || val < -0x8000 {
		encoding = 4
	}
	if val > 0x7FFFFFFF || val < -0x80000000 {
		encoding = 8
	}
	return encoding
}

func (set *IntSet) Remove(val any) int {
	value, err := transVal2Int64(val)
	if err != nil {
		return 0
	}
	found, pos := set.intsetSearch(value)
	if !found {
		return 0
	}
	start := pos * int(set.encoding)
	stop := start + int(set.encoding)
	set.contents = slices.Delete(set.contents, start, stop)
	set.length--
	return 1
}

func (set *IntSet) Contains(val any) bool {
	value, err := transVal2Int64(val)
	if err != nil {
		return false
	}
	found, _ := set.intsetSearch(value)
	return found
}

func (set *IntSet) Len() int {
	return int(set.length)
}

func (set *IntSet) ForEach(consumer func(member any) bool) {
	for i := 0; i < set.Len(); i++ {
		val := set.get(i)
		if !consumer(val) {
			return
		}
	}
}

func (set *IntSet) RandomMembers(n int) any {
	if n < 0 {
		return nil
	}
	if n >= set.Len() {
		return set.ToSlice()
	}
	results := make([]int64, n)
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < n; i++ {
		index := nR.Intn(set.Len())
		results[i] = set.get(index)
	}
	return results
}

func (set *IntSet) ToSlice() any {
	slice := make([]int64, 0, set.Len())
	set.ForEach(func(member any) bool {
		slice = append(slice, member.(int64))
		return true
	})
	return slice
}

func (set *IntSet) RandomDistinctMembers(n int) any {
	if n < 0 {
		return nil
	}
	if n >= set.Len() {
		return set.ToSlice()
	}
	vis := make(map[int64]struct{})
	results := make([]int64, n)
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < n; {
		index := nR.Intn(set.Len())
		val := set.get(index)
		if _, ok := vis[val]; ok {
			continue
		}
		vis[val] = struct{}{}
		results[i] = val
		i++
	}
	return results
}

// NewIntSet 创建一个新的 IntSet
func NewIntSet() *IntSet {
	return &IntSet{
		encoding: 2, // 默认使用 int16 类型
		length:   0,
		contents: make([]byte, 0),
	}
}

// Print 打印集合内容
func (set *IntSet) print() {
	fmt.Printf("IntSet encoding: %d-bit, length: %d, contents: ", set.encoding*8, set.Len())
	for i := 0; i < set.Len(); i++ {
		var val any
		switch set.encoding {
		case 2:
			val = *(*int16)(unsafe.Pointer(&set.contents[i*2]))
		case 4:
			val = *(*int32)(unsafe.Pointer(&set.contents[i*4]))
		case 8:
			val = *(*int64)(unsafe.Pointer(&set.contents[i*8]))
		}
		fmt.Printf("%d ", val)
	}
	fmt.Println()
}

func (set *IntSet) Clone() Set {
	clone := NewIntSet()
	set.ForEach(func(member any) bool {
		clone.Add(member)
		return true
	})
	return clone
}
