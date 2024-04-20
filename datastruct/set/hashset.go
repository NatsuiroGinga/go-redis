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
func (set *HashSet) Add(val string) int {
	return set.hashTable.Set(val, nil)
}

// Remove 从集合中删除一个val
//
// 删除成功返回1, 否则返回0
func (set *HashSet) Remove(val string) int {
	return set.hashTable.Remove(val)
}

// Contains 判断集合中是否包含val
//
// 包含返回true, 否则返回false
func (set *HashSet) Contains(val string) bool {
	_, exist := set.hashTable.Get(val)
	return exist
}

// Len 返回集合中元素的数量
func (set *HashSet) Len() int {
	return set.hashTable.Len()
}

// ToSlice 把集合转化为切片
func (set *HashSet) ToSlice() []string {
	slice := make([]string, 0, set.Len())
	set.hashTable.ForEach(func(key string, val any) bool {
		slice = append(slice, key)
		return true
	})
	return slice
}

// ForEach 遍历集合中的元素
func (set *HashSet) ForEach(consumer func(member string) bool) {
	set.hashTable.ForEach(func(key string, val any) bool {
		return consumer(key)
	})
}

// clone 对集合进行浅拷贝
func (set *HashSet) clone() *HashSet {
	result := NewHashSet()
	set.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	return result
}

// RandomMembers 返回字典中n个随机的 key
func (set *HashSet) RandomMembers(n int) []string {
	return set.hashTable.RandomKeys(n)
}

// RandomDistinctMembers 返回字典中n个随机且不重复的key
func (set *HashSet) RandomDistinctMembers(n int) []string {
	return set.hashTable.RandomDistinctKeys(n)
}

// Intersect 对多个集合求交集
func Intersect(sets ...*HashSet) *HashSet {
	result := NewHashSet()
	if len(sets) == 0 {
		return result
	}

	countMap := make(map[string]int)
	for _, set := range sets {
		set.ForEach(func(member string) bool {
			countMap[member]++
			return true
		})
	}
	for k, v := range countMap {
		if v == len(sets) {
			result.Add(k)
		}
	}
	return result
}

// Union 对多个集合求交集
func Union(sets ...*HashSet) *HashSet {
	result := NewHashSet()
	for _, set := range sets {
		set.ForEach(func(member string) bool {
			result.Add(member)
			return true
		})
	}
	return result
}

// Diff 求多个集合中不同的元素
func Diff(sets ...*HashSet) *HashSet {
	if len(sets) == 0 {
		return NewHashSet()
	}
	result := sets[0].clone()
	for i := 1; i < len(sets) && result.Len() != 0; i++ {
		sets[i].ForEach(func(member string) bool {
			result.Remove(member)
			return true
		})
	}
	return result
}
