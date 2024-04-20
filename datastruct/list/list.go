package list

// Expected check whether given item is equals to expected value
type Expected func(a any) bool

// Consumer traverses list.
// It receives index and value as params, returns true to continue traversal, while returns false to break
type Consumer func(i int, v any) bool

type List interface {
	// PushBack 向list尾插入节点
	PushBack(val any)
	// Get 根据index获取一个节点
	Get(index int) (val any)
	// Set 设置index位置的节点的值为val
	Set(index int, val any)
	// Insert 在index位置插入一个值为val的节点
	Insert(index int, val any)
	// Remove 删除位置为index的节点
	Remove(index int) (val any)
	// RemoveLast 删除list末尾的节点
	RemoveLast() (val any)
	// RemoveAllByVal 删除所有值满足一定条件的节点
	RemoveAllByVal(expected Expected) int
	// RemoveByVal 删除count个值满足一定条件的节点
	RemoveByVal(expected Expected, count int) int
	// ReverseRemoveByVal 倒序删除
	ReverseRemoveByVal(expected Expected, count int) int
	// Len 返回list中节点的数量
	Len() int
	// ForEach 遍历list中的所有节点
	ForEach(consumer Consumer)
	// Contains 判断list中是否有满足一定条件的节点
	Contains(expected Expected) bool
	// Range 返回下标为[start, stop)范围内的索引
	Range(start, stop int) []any
}
