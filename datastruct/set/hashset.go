package set

import (
	"go-redis/datastruct/dict"
)

// HashSet 是一个由HashMap实现的string类型的无序集合
type HashSet struct {
	hashTable dict.Dict
}

func NewHashSet(members ...string) *HashSet {
	set := &HashSet{dict.NewNormalDict()}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

// Add 向集合中添加一个val
//
// 成功添加返回1, 否则返回0
func (set *HashSet) Add(val any) int {
	key, ok := val.(string)
	if !ok {
		return 0
	}
	return set.hashTable.Set(key, nil)
}

// Remove 从集合中删除一个val
//
// 删除成功返回1, 否则返回0
func (set *HashSet) Remove(val any) int {
	key, ok := val.(string)
	if !ok {
		return 0
	}
	return set.hashTable.Remove(key)
}

// Contains 判断集合中是否包含val
//
// 包含返回true, 否则返回false
func (set *HashSet) Contains(val any) bool {
	key, ok := val.(string)
	if !ok {
		return false
	}
	_, exist := set.hashTable.Get(key)
	return exist
}

// Len 返回集合中元素的数量
func (set *HashSet) Len() int {
	return set.hashTable.Len()
}

// ToSlice 把集合转化为切片
func (set *HashSet) ToSlice() any {
	slice := make([]string, 0, set.Len())
	set.hashTable.ForEach(func(key string, _ any) bool {
		slice = append(slice, key)
		return true
	})
	return slice
}

// ForEach 遍历集合中的元素
func (set *HashSet) ForEach(consumer func(member any) bool) {
	set.hashTable.ForEach(func(key string, _ any) bool {
		return consumer(key)
	})
}

// Clone 对集合进行浅拷贝
func (set *HashSet) Clone() Set {
	result := NewHashSet()
	set.ForEach(func(member any) bool {
		result.Add(member)
		return true
	})
	return result
}

// RandomMembers 返回字典中n个随机的 key
func (set *HashSet) RandomMembers(n int) any {
	return set.hashTable.RandomKeys(n)
}

// RandomDistinctMembers 返回字典中n个随机且不重复的key
func (set *HashSet) RandomDistinctMembers(n int) any {
	return set.hashTable.RandomDistinctKeys(n)
}
