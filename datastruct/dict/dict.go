package dict

type consumer func(key string, value any) bool

// Dict 是一个Hash字典的接口
type Dict interface {
	// Get 返回 key 对应的 value 和 ok, 并发不安全
	//
	// 如果 key 不存在，ok 为 false
	Get(key string) (value any, isExist bool)

	// GetWithLock 同Get, 并发安全
	GetWithLock(key string) (value any, isExist bool)

	// Set 设置 key 对应的 value, 并发不安全
	//
	// 如果 key 已经存在，做更新, 返回 0
	Set(key string, value any) (n int)

	// SetWithLock 同Set, 并发安全
	SetWithLock(key string, value any) (n int)

	// Len 返回字典的长度
	Len() (n int)
	// PutIfAbsent 设置 key 对应的 value, 当且仅当 key 不存在时, 并发不安全
	//
	// 返回 1 表示 key 不存在，返回 0 表示 key 已经存在
	PutIfAbsent(key string, value any) (n int)

	// PutIfAbsentWithLock 同PutIfAbsent, 并发安全
	PutIfAbsentWithLock(key string, value any) (n int)

	// PutIfExist 设置 key 对应的 value, 当且仅当 key 存在时, 并发不安全
	//
	// 返回 1 表示 key 存在，返回 0 表示 key 不存在
	PutIfExist(key string, value any) (n int)

	// PutIfExistWithLock 同PutIfExist, 并发安全
	PutIfExistWithLock(key string, value any) (n int)

	// Remove 删除 key 对应的 value, 并发不安全
	//
	// 返回 1 表示 key 存在，返回 0 表示 key 不存在
	Remove(key string) (n int)

	// RemoveWithLock 同Remove, 并发安全
	RemoveWithLock(key string) (n int)

	// ForEach 遍历字典中的每一个元素, 并发安全
	ForEach(consumer consumer)

	// Keys 返回字典中所有的 key, 并发安全
	Keys() (keys []string)

	// RandomKeys 返回字典中n个随机的 key, 允许重复的key, 并发安全
	RandomKeys(n int) (keys []string)

	// RandomDistinctKeys 返回字典中n个随机的不重复的 key, 并发安全
	//
	// 如果n > len(dict), 则返回dict中的所有key
	RandomDistinctKeys(n int) (keys []string)

	// Clear 清空字典, 并发安全
	Clear()

	// RWLocks 对要读/写的键分别上读/写锁, 允许重复的键
	//
	// writeKeys: 要写入的键; readKeys: 要读的键
	RWLocks(writeKeys, readKeys []string)

	// RWUnLocks 对要读/写的键分别解除读/写锁, 允许重复的键
	//
	// writeKeys: 要写入的键; readKeys: 要读的键
	RWUnLocks(writeKeys, readKeys []string)
}
