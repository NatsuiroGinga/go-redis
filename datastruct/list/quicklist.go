package list

import (
	"container/list"
	"slices"

	"go-redis/config"
	"go-redis/enum"
	"go-redis/lib/logger"
)

// QuickList 是一个双向链表, 其中的每一个节点是一个存储任意类型的不定长数组, 可以存储不同类型的元素
type QuickList struct {
	data      *list.List // 存储所有shard的list
	size      int        // shard的数量
	shardSize int        // 每一个shard的大小
}

// createShardAndPush 在quicklist中创建新的shard, 然后插入新数据
//
// 一般在quicklist中的所有shard的长度达到限制的时候使用
func (ql *QuickList) createShardAndPush(val any) {
	shard := make([]any, 0, ql.shardSize)
	shard = append(shard, val)
	ql.data.PushBack(shard)
}

// find 根据index查找目标节点所在的shard
//
// 如果 index < ql.size/2, 从链表头开始查找; 如果 index > ql.size / 2, 从链表尾开始查找
//
// 返回shard的iterator
func (ql *QuickList) find(index int) *iterator {
	if ql == nil {
		logger.Fatal(enum.LIST_IS_NIL)
	}
	if index < 0 || index >= ql.size {
		logger.Fatal(enum.INDEX_OUT_OF_RANGE)
	}
	var n *list.Element
	var shard []any
	var shardBeg int
	if index < ql.size/2 {
		n = ql.data.Front()
		shardBeg = 0
		for {
			shard = n.Value.([]any)
			if shardBeg+len(shard) > index {
				break
			}
			shardBeg += len(shard)
			n = n.Next()
		}
	} else {
		n = ql.data.Back()
		shardBeg = ql.size
		for {
			shard = n.Value.([]interface{})
			shardBeg -= len(shard)
			if shardBeg <= index {
				break
			}
			n = n.Prev()
		}
	}
	shardOffset := index - shardBeg
	return &iterator{
		node:   n,
		offset: shardOffset,
		ql:     ql,
	}
}

func (ql *QuickList) PushBack(val any) {
	// 1. size加1
	ql.size++
	// 2. quicklist的数据list为空, 那么新建一个Shard并且插入数据
	if ql.data.Len() == 0 {
		ql.createShardAndPush(val)
		return
	}
	// 3. 找到quicklist的最后一个shard
	backNode := ql.data.Back()
	backShard := backNode.Value.([]any)
	// 4. 最后一个Shard的长度达到config中的限制, 新建Shard
	if len(backShard) == cap(backShard) {
		ql.createShardAndPush(val)
		return
	}
	// 5. 向shard插入数据
	backShard = append(backShard, val)
	backNode.Value = backShard
}

func (ql *QuickList) Get(index int) (val any) {
	iter := ql.find(index)
	return iter.get()
}

func (ql *QuickList) Set(index int, val any) {
	iter := ql.find(index)
	iter.set(val)
}

func (ql *QuickList) Insert(index int, val any) {
	// 1. 执行pushback
	if index == ql.size {
		ql.PushBack(val)
		return
	}
	// 2. 找到index所指的Shard
	iter := ql.find(index)
	shard := iter.node.Value.([]any)
	// 3. 判断shard的长度是否达到容量限制
	if len(shard) < cap(shard) {
		shard = slices.Insert(shard, iter.offset, val)
		iter.node.Value = shard
		ql.size++
		return
	}
	// 4. len(shard) == cap(shard), 那么要在两个shard中间新建一个shard, 把val插入新建的shard中
	var nextShard []any
	// 4.1 把当前Shard的数据的后一半给nextShard
	nextShard = append(nextShard, shard[ql.shardSize/2:]...)
	// 4.2 当前Shard的数据只保留前一半
	shard = shard[:ql.shardSize/2]
	// 4.3 如果目标节点在shard的前半部分, 插入Shard内
	if iter.offset < len(shard) {
		shard = slices.Insert(shard, iter.offset, val)
	} else { // 4.4 如果目标节点在shard的后半部分, 插入nextShard内
		i := iter.offset - ql.shardSize/2
		nextShard = slices.Insert(nextShard, i)
	}
	// 5. 保存shard和nextShard
	iter.node.Value = shard
	ql.data.InsertAfter(nextShard, iter.node)
	ql.size++
}

func (ql *QuickList) Remove(index int) (val any) {
	shard := ql.find(index)
	return shard.remove()
}

func (ql *QuickList) RemoveLast() (val any) {
	if ql.Len() == 0 {
		return nil
	}
	ql.size--
	// 1. 找到最后一个shard
	lastNode := ql.data.Back()
	lastShard := lastNode.Value.([]any)
	// 2. 如果shard只有一个节点, 就删除这个shard
	if len(lastShard) == 1 {
		ql.data.Remove(lastNode)
		return lastShard[0]
	}
	// 3. shard内的节点 > 1, 删除节点
	val = lastShard[len(lastShard)-1]
	lastShard = lastShard[:len(lastShard)-1]
	lastNode.Value = lastShard
	return val
}

func (ql *QuickList) RemoveAllByVal(expected Expected) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
		} else {
			iter.next()
		}
	}
	return removed
}

func (ql *QuickList) RemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() && removed < count {
		if expected(iter.get()) {
			iter.remove()
			removed++
		} else {
			iter.next()
		}
	}
	return removed
}

func (ql *QuickList) ReverseRemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(ql.size - 1)
	removed := 0
	for !iter.atBegin() && removed < count {
		if expected(iter.get()) {
			iter.remove()
			removed++
		}
		iter.prev()
	}
	return removed
}

func (ql *QuickList) Len() int {
	return ql.size
}

func (ql *QuickList) ForEach(consumer Consumer) {
	if ql == nil {
		logger.Fatal(enum.LIST_IS_NIL)
	}
	if ql.Len() == 0 {
		return
	}
	iter := ql.find(0)
	i := 0
	for consumer(i, iter.get()) && iter.next() {
		i++
	}
}

func (ql *QuickList) Contains(expected Expected) bool {
	contains := false
	ql.ForEach(func(i int, actual any) bool {
		if expected(actual) {
			contains = true
			return false
		}
		return true
	})
	return contains
}

func (ql *QuickList) Range(start, stop int) []any {
	if start < 0 || start >= ql.Len() || stop < start || stop > ql.Len() {
		logger.Fatal(enum.INDEX_OUT_OF_RANGE)
	}
	sliceSize := stop - start
	slice := make([]any, sliceSize)
	iter := ql.find(start)
	for i := 0; i < sliceSize; i++ {
		slice[i] = iter.get()
		iter.next()
	}
	return slice
}

func NewQuickList() *QuickList {
	l := &QuickList{
		data:      list.New(),
		shardSize: config.Properties.ListMaxShardSize,
	}
	return l
}
