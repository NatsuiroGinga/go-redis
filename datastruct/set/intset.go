package set

import (
	"fmt"
	"math/rand"
	"slices"
	"time"
	"unsafe"
)

// IntSet 是一个整数集合，可以存储 int16, int32 或 int64 类型的整数
type IntSet struct {
	encoding uint32 // 表示当前存储的整数类型
	length   uint32 // 集合中元素的数量
	contents []byte // 存储元素的切片
}

func (set *IntSet) Add(value int64) int {
	// 1. 确定新值的编码类型
	newEncoding := getValEncoding(value)

	// 2. 如果需要，升级现有元素
	if newEncoding > set.encoding {
		newContents := make([]byte, set.length*newEncoding)
		for i := uint32(0); i < set.length; i++ {
			val := set.get(int(i))
			*(*int64)(unsafe.Pointer(&newContents[i*newEncoding])) = val
		}
		set.contents = newContents
		set.encoding = newEncoding
	}
	// 3. 找到元素所在位置
	found, pos := set.intsetSearch(value)
	if found { // 3.1 元素已经存在, 不做插入, 返回
		return 0
	}

	// 4. 添加新元素
	set.contents = slices.Insert(set.contents, pos, make([]byte, newEncoding)...)
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

// intsetSearch 在 IntSet 中查找给定的整数值
//
// 如果找到了值, 返回下标; 否则返回可以插入值的位置
func (set *IntSet) intsetSearch(value int64) (found bool, pos int) {
	minIndex := 0
	maxIndex := int(set.length) - 1
	var mid int
	var midVal int64

	for minIndex <= maxIndex {
		mid = (minIndex + maxIndex) / 2
		midVal = set.get(mid)

		if midVal == value {
			return true, mid // 找到值
		} else if midVal < value {
			minIndex = mid + 1
		} else {
			maxIndex = mid - 1
		}
	}

	return false, minIndex // 没找到值，返回应该插入的位置
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
func getValEncoding(val int64) uint32 {
	encoding := uint32(2)
	if val > 0x7FFF || val < -0x8000 {
		encoding = 4
	}
	if val > 0x7FFFFFFF || val < -0x80000000 {
		encoding = 8
	}
	return encoding
}

func (set *IntSet) Remove(val int64) int {
	found, pos := set.intsetSearch(val)
	if !found {
		return 0
	}
	set.contents = slices.Delete(set.contents, pos, pos+int(set.encoding))
	set.length--
	return 1
}

func (set *IntSet) Contains(val int64) bool {
	found, _ := set.intsetSearch(val)
	return found
}

func (set *IntSet) Len() int {
	return int(set.length)
}

func (set *IntSet) ForEach(consumer func(member int64) bool) {
	for i := 0; i < int(set.length); i++ {
		val := set.get(i)
		if !consumer(val) {
			return
		}
	}
}

func (set *IntSet) RandomMembers(n int) []int64 {
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

func (set *IntSet) ToSlice() []int64 {
	slice := make([]int64, 0, set.Len())
	set.ForEach(func(member int64) bool {
		slice = append(slice, member)
		return true
	})
	return slice
}

func (set *IntSet) RandomDistinctMembers(n int) []int64 {
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
	fmt.Printf("IntSet encoding: %d-bit, length: %d, contents: ", set.encoding*8, set.length)
	for i := uint32(0); i < set.length; i++ {
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
