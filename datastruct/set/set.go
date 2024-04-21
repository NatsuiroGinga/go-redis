package set

type dataType interface {
	intDataType | ~string
}

type intDataType interface {
	~int16 | ~int32 | ~int64
}

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
