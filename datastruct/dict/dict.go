package dict

type Consumer func(key string, value any)

// Dict 是一个字典接口
type Dict interface {
	// Get 返回 key 对应的 value 和 ok
	//
	// 如果 key 不存在，ok 为 false
	Get(key string) (value any, ok bool)
	// Set 设置 key 对应的 value
	//
	// 如果 key 已经存在，做更新, 返回 0
	Set(key string, value any) (n int)
	// Len 返回字典的长度
	Len() (n int)
	// PutIfAbsent 设置 key 对应的 value, 当且仅当 key 不存在时
	//
	// 返回 1 表示 key 不存在，返回 0 表示 key 已经存在
	PutIfAbsent(key string, value any) (n int)
	// PutIfExist 设置 key 对应的 value, 当且仅当 key 存在时
	//
	// 返回 1 表示 key 存在，返回 0 表示 key 不存在
	PutIfExist(key string, value any) (n int)
	// Remove 删除 key 对应的 value
	//
	// 返回 1 表示 key 存在，返回 0 表示 key 不存在
	Remove(key string) (n int)
	// ForEach 遍历字典中的每一个元素
	ForEach(consumer Consumer)
	// Keys 返回字典中所有的 key
	Keys() []string
	// RandomKeys 返回字典中n个随机的 key
	RandomKeys(n int) []string
	// RandomDistinctKeys 返回字典中n个随机的不重复的 key
	RandomDistinctKeys(n int) []string
	// Clear 清空字典
	Clear()
}
