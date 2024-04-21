package set

import (
	"strconv"

	"go-redis/datastruct/dict"
	"go-redis/lib/logger"
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

// Intersect 对多个集合求交集
func Intersect(sets ...Set) Set {
	result := NewHashSet()
	if len(sets) == 0 {
		return result
	}

	countMap := make(map[string]int)
	for _, set := range sets {
		set.ForEach(func(member any) bool {
			if _, ok := member.(string); ok {
				countMap[member.(string)]++
			} else {
				countMap[strconv.FormatInt(member.(int64), 10)]++
			}
			return true
		})
	}

	isAllInt := true
	for k, v := range countMap {
		if v == len(sets) {
			if isAllInt {
				_, err := strconv.ParseInt(k, 10, 64)
				if err != nil {
					isAllInt = false
				}
			}
			result.Add(k)
		}
	}

	// 如果交集全部都是数字, 把hashset转化为intset
	if isAllInt {
		return HashSet2IntSet(result)
	}

	return result
}

// Union 对多个集合求交集
func Union(sets ...Set) Set {
	var result Set = NewIntSet()
	if len(sets) == 0 {
		return result
	}

	for _, set := range sets {
		set.ForEach(func(k any) bool {
			if kStr, ok := k.(string); ok {
				if intResult, succ := result.(*IntSet); succ {
					result = IntSet2HashSet(intResult)
				}
				result.Add(kStr)
			} else {
				num := k.(int64)
				switch result.(type) {
				case *IntSet:
					result.Add(num)
				case *HashSet:
					result.Add(strconv.FormatInt(num, 10))
				default:
					logger.Fatal("unknown type")
					return false
				}
			}
			return true
		})
	}
	return result
}

// Diff 求多个集合中不同的元素
func Diff(sets ...Set) Set {
	if len(sets) == 0 {
		return NewIntSet()
	}
	result := sets[0].Clone()
	_, isIntSet := result.(*IntSet)

	for i := 1; i < len(sets) && result.Len() != 0; i++ {
		sets[i].ForEach(func(member any) bool {
			_, ok := sets[i].(*IntSet)
			if ok && !isIntSet { // 第一个集合是hashSet, 此集合是intset
				result.Remove(strconv.FormatInt(member.(int64), 10))
			}
			if !ok && isIntSet { // 第一个集合是intset, 此集合是hashset
				num, err := strconv.ParseInt(member.(string), 10, 64)
				if err != nil {
					return true
				}
				result.Remove(num)
			}
			// 第一个集合和此集合都是hashset 或者 第一个集合和此集合都是intset
			result.Remove(member)
			return true
		})
	}
	return result
}

// IntSet2HashSet 把intset转化为hashset
func IntSet2HashSet(intSet *IntSet) (hashSet *HashSet) {
	hashSet = NewHashSet()
	if intSet == nil || intSet.Len() == 0 {
		return
	}
	intSet.ForEach(func(member any) bool {
		num, ok := member.(int64)
		if !ok {
			return false
		}
		str := strconv.FormatInt(num, 10)
		hashSet.Add(str)
		return true
	})
	return
}

func HashSet2IntSet(hashSet *HashSet) (intSet *IntSet) {
	intSet = NewIntSet()
	if hashSet == nil || (hashSet.Len() == 0) {
		return
	}
	hashSet.ForEach(func(member any) bool {
		memberStr, ok := member.(string)
		if !ok {
			return false
		}
		str, err := strconv.ParseInt(memberStr, 10, 64)
		if err != nil {
			logger.Error(err)
			return false
		}
		intSet.Add(str)
		return true
	})
	return
}
