package set

import (
	"strconv"

	"go-redis/lib/logger"
	"go-redis/lib/utils"
)

// Set 大多数情况下是String类型的无序集合, 底层用哈希表实现
//
// 当Set中所有元素均为整数且元素个数不超过设定阈值时, 为int类型的有序集合, 底层用数组实现, 集合成员是唯一的
type Set interface {
	Add(val any) int
	Remove(val any) int
	Contains(val any) bool
	Len() int
	ForEach(consumer func(member any) bool)
	RandomMembers(n int) any         // 返回n个随机的值, 可重复
	RandomDistinctMembers(n int) any // 返回n个随机的值, 不可重复
	ToSlice() any                    // 返回集合中所有值
	Clone() Set
}

// ToBytes 把int64或者string类型的值转化为[]byte
func ToBytes(val any) []byte {
	switch val.(type) {
	case string:
		return utils.String2Bytes(val.(string))
	case int64:
		return utils.String2Bytes(strconv.FormatInt(val.(int64), 10))
	default:
		return nil
	}
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
		return hashSet2IntSet(result)
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

func hashSet2IntSet(hashSet *HashSet) (intSet *IntSet) {
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
