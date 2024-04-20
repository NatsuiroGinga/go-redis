package dict

import (
	"hash"
	"hash/fnv"
	"math"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go-redis/datastruct/sortedset"
	"go-redis/enum"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
)

// ConcurrentDict 并发安全的字典
type ConcurrentDict struct {
	buckets    []*shard    // 固定数量的shard存储数据
	count      int32       // 字典中存储的key-value的数量
	shardCount int         // shard的数量
	h          hash.Hash32 // hash函数
}

func (dict *ConcurrentDict) Get(key string) (val any, isExist bool) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	sd := dict.getShard(key)

	val, isExist = sd.m[key]
	return
}

func (dict *ConcurrentDict) GetWithLock(key string) (val any, isExist bool) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	sd := dict.getShard(key)

	sd.mu.RLock()
	defer sd.mu.RUnlock()

	val, isExist = sd.m[key]
	return
}

func (dict *ConcurrentDict) Set(key string, value any) (n int) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	// 1. 获取key所在的shard
	sd := dict.getShard(key)
	// 2. 判断key是否已经存在
	if _, ok := sd.m[key]; ok {
		sd.m[key] = value
		return 0
	}
	// 3. 键值对的数量加1
	atomic.AddInt32(&dict.count, 1)
	// 4. 改变数值
	sd.m[key] = value

	return 1
}

func (dict *ConcurrentDict) SetWithLock(key string, value any) (n int) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	// 1. 获取key所在的shard
	sd := dict.getShard(key)
	// 2. 上写锁
	sd.mu.RLock()
	defer sd.mu.RUnlock()
	// 3. 判断key是否已经存在
	if _, ok := sd.m[key]; ok {
		sd.m[key] = value
		return 0
	}
	// 4. 键值对的数量加1
	dict.count++
	// 5. 改变数值
	sd.m[key] = value

	return 1
}

func (dict *ConcurrentDict) Len() (n int) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	// 使用原子方法获取dict.count保证并发安全
	return int(atomic.LoadInt32(&dict.count))
}

func (dict *ConcurrentDict) PutIfAbsent(key string, value any) (n int) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	sd := dict.getShard(key)
	// 如果key已经存在, 返回0
	if _, ok := sd.m[key]; ok {
		return 0
	}
	atomic.AddInt32(&dict.count, 1)
	sd.m[key] = value
	return 1
}

func (dict *ConcurrentDict) PutIfAbsentWithLock(key string, value any) (n int) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	sd := dict.getShard(key)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	if _, ok := sd.m[key]; ok {
		return 0
	}
	dict.count++
	sd.m[key] = value
	return 1
}

func (dict *ConcurrentDict) PutIfExist(key string, value any) (n int) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	sd := dict.getShard(key)
	// 如果key不存在, 返回0
	if _, ok := sd.m[key]; !ok {
		return 0
	}
	atomic.AddInt32(&dict.count, 1)
	sd.m[key] = value
	return 1
}

func (dict *ConcurrentDict) PutIfExistWithLock(key string, value any) (n int) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	sd := dict.getShard(key)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	if _, ok := sd.m[key]; !ok {
		return 0
	}
	dict.count++
	sd.m[key] = value
	return 1
}

func (dict *ConcurrentDict) Remove(key string) (n int) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	sd := dict.getShard(key)
	if _, ok := sd.m[key]; !ok {
		return 0
	}
	delete(sd.m, key)
	atomic.AddInt32(&dict.count, -1)
	return 1
}

func (dict *ConcurrentDict) RemoveWithLock(key string) (n int) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	sd := dict.getShard(key)

	sd.mu.Lock()
	defer sd.mu.Unlock()

	// 如果key不存在, 返回0
	if _, ok := sd.m[key]; !ok {
		return 0
	}
	delete(sd.m, key)
	dict.count--
	return 1
}

func (dict *ConcurrentDict) ForEach(consumer consumer) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	// 1. 遍历字典内所有桶
	for _, sd := range dict.buckets {
		// 2. 对一个桶上读锁
		sd.mu.RLock()
		// 3. 定义处理桶内数据的函数
		f := func() bool {
			defer sd.mu.RUnlock()
			for key, value := range sd.m {
				continues := consumer(key, value)
				if !continues {
					return false
				}
			}
			return true
		}
		// 4. 根据处理函数返回值决定是否结束
		if !f() {
			break
		}
	}
}

func (dict *ConcurrentDict) Keys() (keys []string) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	// 1. 预先分配容量
	keys = make([]string, 0, dict.Len())
	// 2. 遍历字典所有的键, forEach方法加了读锁
	dict.ForEach(func(key string, val any) bool {
		keys = append(keys, key)
		return true
	})
	return
}

func (dict *ConcurrentDict) RandomKeys(n int) (keys []string) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	if n < 0 {
		return nil
	}
	// 1. 检查所需key的数量是否大于字典现存的key数量
	if n >= dict.Len() {
		return dict.Keys()
	}
	// 2. 预分配容量
	keys = make([]string, 0, n)
	// 3. 以现在的系统时间创建随机数
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	// 4. 遍历
	for i := 0; i < n; {
		sd := dict.buckets[nR.Intn(dict.shardCount)]
		if sd == nil {
			continue
		}
		key := sd.RandomKey()
		if key != "" {
			keys = append(keys, key)
		}
	}
	return
}

// RandomKey returns a key randomly
func (shard *shard) RandomKey() string {
	if shard == nil {
		logger.Fatal(enum.SHARD_IS_NIL)
	}

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	for key := range shard.m {
		return key
	}

	return ""
}

func (dict *ConcurrentDict) RandomDistinctKeys(n int) (keys []string) {
	if n < 0 {
		return nil
	}
	size := dict.Len()
	if n >= size {
		return dict.Keys()
	}

	result := make(map[string]struct{})
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(result) < n {
		shardIndex := uint32(nR.Intn(dict.shardCount))
		s := dict.buckets[shardIndex]
		if s == nil {
			continue
		}
		key := s.RandomKey()
		if key != "" {
			if _, exists := result[key]; !exists {
				result[key] = struct{}{}
			}
		}
	}
	keys = make([]string, 0, n)
	for k := range result {
		keys = append(keys, k)
	}

	return
}

func (dict *ConcurrentDict) Clear() {
	*dict = *NewConcurrentDict(dict.shardCount)
}

// shard 存储key-value数据
type shard struct {
	m  map[string]any // 实际存储数据的map
	mu sync.RWMutex   // 读写锁
}

// computeCapacity 计算shard的数量, 默认16
func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

// hash 计算key的hash值
func (dict *ConcurrentDict) hash(key string) uint32 {
	// 1. 延迟重置hash算法
	defer dict.h.Reset()
	// 2. 写入key
	_, err := dict.h.Write(utils.String2Bytes(key))
	if err != nil {
		logger.Error("hash fail:", err.Error())
	}
	// 3. 执行顺序: 计算hash值保存到栈 -> 执行defer语句 -> 函数返回
	return dict.h.Sum32()
}

// NewConcurrentDict 通过给定的shard数量创建dict
func NewConcurrentDict(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]any),
		}
	}
	dict := &ConcurrentDict{
		count:      0,
		buckets:    table,
		shardCount: shardCount,
		h:          fnv.New32(),
	}
	return dict
}

// computeSlot 计算hash值对应的bucket号
func (dict *ConcurrentDict) computeSlot(hashCode uint32) uint32 {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	tableSize := uint32(dict.shardCount)
	return (tableSize - 1) & hashCode
}

// getShard 根据key获取一个shard
//
// key: 键
//
// 返回key所在的shard
func (dict *ConcurrentDict) getShard(key string) *shard {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	hashCode := dict.hash(key)
	slot := dict.computeSlot(hashCode)
	return dict.buckets[slot]
}

// toLockIndices 对于要上锁的键, 获取保存它们的shard的索引数组, 允许重复键
//
// keys: 键的数组; reverse: 是否逆序
//
// 返回保存keys的shard的索引数组
func (dict *ConcurrentDict) toLockIndices(keys []string, reverse bool) (indices []uint32) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	if len(keys) == 0 {
		return nil
	}
	zset := sortedset.NewSortedSet()
	for _, key := range keys {
		index := dict.computeSlot(dict.hash(key))
		zset.Add(key, float64(index))
	}
	zset.ForEachByRank(0, zset.Length(), reverse, func(e *sortedset.Element) bool {
		indices = append(indices, uint32(e.Score))
		return true
	})
	// 1. 索引map
	// indexMap := make(map[uint32]struct{})
	// 2. 遍历keys, 获取保存每一个key的shard的索引
	// indexMap[index] = struct{}{}
	// 3. 确保索引不重复
	/*indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	// 4. 排序, 确保按照给定key的顺序来获取锁, 以免造成并发问题
	// 在锁定多个key时需要注意，若协程A持有键a的锁试图获得键b的锁，此时协程B持有键b的锁试图获得键a的锁则会形成死锁。
	// 解决方法是所有协程都按照相同顺序加锁，若两个协程都想获得键a和键b的锁，那么必须先获取键a的锁后获取键b的锁，这样就可以避免循环等待。
	utils.OrderSort(indices, reverse)*/
	return indices
}

func (dict *ConcurrentDict) RWLocks(writeKeys, readKeys []string) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	if len(writeKeys) == 0 && len(readKeys) == 0 {
		return
	}
	// 1. 拼接writeKeys和readKeys
	keys := slices.Concat(writeKeys, readKeys)
	// 2. 获取所有key所在的shard索引数组
	indices := dict.toLockIndices(keys, false)
	// 3. 把writeKeys放入集合, 方便后面筛选
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := dict.computeSlot(dict.hash(wKey))
		writeIndexSet[idx] = struct{}{}
	}
	// 4. 遍历所有索引, 如果是writeKey则上写锁, 如果是readKey则上读锁
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.buckets[index].mu
		if w {
			mu.Lock()
		} else {
			mu.RLock()
		}
	}
}

func (dict *ConcurrentDict) RWUnLocks(writeKeys, readKeys []string) {
	if dict == nil {
		logger.Fatal(enum.DICT_IS_NIL)
	}
	if len(writeKeys) == 0 && len(readKeys) == 0 {
		return
	}
	keys := slices.Concat(writeKeys, readKeys)
	indices := dict.toLockIndices(keys, true)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := dict.computeSlot(dict.hash(wKey))
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.buckets[index].mu
		if w {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}
