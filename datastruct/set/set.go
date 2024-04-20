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
type Set[T int64 | string] interface {
	Add(val T) int
	Remove(val T) int
	Contains(val T) bool
	Len() int
	ForEach(consumer func(member T) bool)
	RandomMembers(n int) []T
	RandomDistinctMembers(n int) []T
	ToSlice() []T
}
