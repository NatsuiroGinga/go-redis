package list

import (
	"container/list"
	"slices"
)

// iterator of QuickList, move between [-1, ql.Len()]
type iterator struct {
	node   *list.Element // 指向的节点
	offset int           // 片内偏移量
	ql     *QuickList    // 所属跳表
}

func (iter *iterator) get() any {
	return iter.shard()[iter.offset]
}

func (iter *iterator) shard() []any {
	return iter.node.Value.([]any)
}

// next 判断iterator是否还有下一个shard, 同时把offset加1
//
// 如果iterator所指shard已经是最后一个, 那么返回false
func (iter *iterator) next() bool {
	shard := iter.shard()
	if iter.offset < len(shard)-1 {
		iter.offset++
		return true
	}
	// iter.offset >= len(shard) - 1
	if iter.node == iter.ql.data.Back() {
		// already at last node
		iter.offset = len(shard)
		return false
	}
	iter.offset = 0
	iter.node = iter.node.Next()
	return true
}

// prev 判断iterator是否还有下一个shard, 同时把offset减1
//
// 如果iterator所指节点是第一个, 那么返回false
func (iter *iterator) prev() bool {
	if iter.offset > 0 {
		iter.offset--
		return true
	}
	// move to prev shard
	if iter.node == iter.ql.data.Front() {
		// already at first shard
		iter.offset = -1
		return false
	}
	iter.node = iter.node.Prev()
	prevShard := iter.node.Value.([]any)
	iter.offset = len(prevShard) - 1
	return true
}

func (iter *iterator) set(val interface{}) {
	shard := iter.shard()
	shard[iter.offset] = val
}

func (iter *iterator) atEnd() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Back() {
		return false
	}
	shard := iter.shard()
	return iter.offset == len(shard)
}

func (iter *iterator) atBegin() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Front() {
		return false
	}
	return iter.offset == -1
}

// remove 删除iter所指shard内的节点
func (iter *iterator) remove() any {
	// 1. 删除shard内的节点
	shard := iter.shard()
	val := shard[iter.offset]
	shard = slices.Delete(shard, iter.offset, iter.offset+1)
	// 2. 判断shard被删后是否为空
	if len(shard) > 0 {
		// 2.1 shard不为空, 更新iter所指的shard
		iter.node.Value = shard
		// 2.2 如果被删的节点是shard中最后面的节点 且 iter没有指向quicklist中的最后一个shard
		// 把iter指向下一个shard, 更新offset
		if iter.offset == len(shard) {
			if iter.node != iter.ql.data.Back() {
				iter.node = iter.node.Next()
				iter.offset = 0
			}
			// else: assert iter.atEnd() == true
		}
	} else {
		// 2.3 shard为空且是quicklist中的最后一个shard
		if iter.node == iter.ql.data.Back() {
			// 2.4 删除shard, 更新iter为前一个shard
			if prevNode := iter.node.Prev(); prevNode != nil {
				iter.ql.data.Remove(iter.node)
				iter.node = prevNode
				iter.offset = len(prevNode.Value.([]any))
			} else {
				// 2.5 当前shard是quicklist中的唯一一个shard, 删除它后, quicklist变为空
				iter.ql.data.Remove(iter.node)
				iter.node = nil
				iter.offset = 0
			}
		} else { // 2.6 shard不是最后一个, 那么就把iter指向下一个shard
			nextNode := iter.node.Next()
			iter.ql.data.Remove(iter.node)
			iter.node = nextNode
			iter.offset = 0
		}
	}
	iter.ql.size--
	return val
}
