package sortedset

import (
	"math/rand"

	"go-redis/lib/utils"
)

const (
	skiplist_maxLevel = 1 << 5 // 最多允许32级指针
	skiplist_p        = 0.25   // 每个位于第 i 层的节点有 p 的概率出现在第 i+1 层，p 为常数
)

// Element 是跳表中每个节点实际存储的元素
type Element struct {
	Ele   string  // 节点存储的值
	Score float64 // 节点分数, 用于排序/查找
}

// level 是一个节点的一层索引
type level struct {
	forward *skiplistNode // 指向下一个score更大的节点的指针
	span    int64         // 索引跨度
}

// skiplistNode 是跳表中的一个节点
type skiplistNode struct {
	Element                // 节点实际存储的值
	backward *skiplistNode // 前一个节点的指针
	level    []*level      // 多级索引数组, level[0]是原始数据
}

func newSkiplistNode(ele string, score float64, l int) (node *skiplistNode) {
	node = &skiplistNode{
		Element: Element{
			ele,
			score,
		},
		level: make([]*level, l),
	}
	for i := range node.level {
		node.level[i] = new(level)
	}
	return
}

// skiplist 跳表支持对数据的快速查找，插入和删除。
//
// 跳表的期望空间复杂度为 O(n)，跳表的查询，插入和删除操作的期望时间复杂度都为 O(log n)。
type skiplist struct {
	header, tail *skiplistNode // 头尾节点指针
	length       int64         // 节点数量
	level        int           // 最大的索引层级, 默认是1
}

func newSkiplist() *skiplist {
	return &skiplist{
		level:  1,
		header: newSkiplistNode("", 0, skiplist_maxLevel),
	}
}

/*
randomLevel 随机生成 1~MAX_LEVEL 之间的数.
该方法有 1/2 的概率返回 1、1/4 的概率返回 2、1/8的概率返回 3，以此类推。

返回 1 表示当前插入的该元素不需要建索引，只需要存储数据到原始链表即可（概率 1/2）

返回 2 表示当前插入的该元素需要建一级索引（概率 1/4）

返回 3 表示当前插入的该元素需要建二级索引（概率 1/8）

返回 4 表示当前插入的该元素需要建三级索引（概率 1/16）
*/
func randomLevel() int {
	lv := 1
	for float32(rand.Int31()&0xFFFF) < skiplist_p*0xFFFF {
		lv++
	}
	return utils.If(lv < skiplist_maxLevel, lv, skiplist_maxLevel)
}

// insert 插入新节点
//
// score值允许相同，但是必须要确保ele不相同；重新插入相同的ele是永远不会发生的，因为插入之前会在hashTable中进行测试该ele是否存在
//
// 插入节点的过程就是先执行一遍查询的过程，
// 中途记录新节点是要插入哪一些节点的后面，最后再执行插入。
// 每一层最后一个键值小于 key 的节点，就是需要进行修改的节点。
//
// 返回新插入的节点
func (list *skiplist) insert(ele string, score float64) *skiplistNode {
	// 1. 记录每一层插入位置的前一个节点
	update := make([]*skiplistNode, skiplist_maxLevel)
	// 2. 记录的是每一层索引中，新节点之前所有节点的span的总和；也就是目标节点在当前层的排名
	rank := make([]int64, skiplist_maxLevel)
	// 3. 寻找每层要插入的位置, 从上到下查询每一层索引
	node := list.header
	for i := list.level - 1; i >= 0; i-- {
		// 3.1 存储每层索引中要插入的位置, 最高层初始化0
		rank[i] = utils.If(i == list.level-1, 0, rank[i+1])
		// 3.2 向后遍历此层节点, 找到最后一个键值小于 key 的节点, 就是需要进行修改的节点
		if node.level[i] != nil {
			// 3.3. 如果下一个节点不为空，并且下一个节点的Score值小于目标score值继续向下一个节点探寻
			// 下一个节点不为空并且下一个节点的Score值等于目标score值，但是下一个节点的ele小于目标ele，继续探寻下一个节点
			for canForward(node.level[i], ele, score) {
				rank[i] += node.level[i].span
				node = node.level[i].forward
			}
		}
		update[i] = node
	}
	// 4. 生成随机数, 决定当前插入的元素要建立几级索引
	lv := randomLevel()
	// 5. 如果生成的随机数大于目前跳表的索引层级, 则扩展跳表的索引
	if lv > list.level {
		for i := list.level; i < lv; i++ {
			// 5.1 初始化rank
			rank[i] = 0
			// 5.2 这一层，将该节点插入到header后面
			update[i] = list.header
			// 5.3 初始化span, 因为此时的第i层并没有实际的节点，先将其初始化为list.length
			update[i].level[i].span = list.length
		}
		list.level = lv
	}

	// 6. 创建新节点
	node = newSkiplistNode(ele, score, lv)
	// 7. 插入每一层索引
	for i := 0; i < lv; i++ {
		// 7.1 新节点的后继是要修改的节点的后继
		node.level[i].forward = update[i].level[i].forward
		// 7.2 要修改的节点的后继是新节点, 完成插入
		update[i].level[i].forward = node

		// 7.3 更新已经修改的节点的索引跨度
		// rank[0]是最底层新节点之前的所有节点span总和
		// rank[i]是当前索引层新节点之前的所有节点span总和
		// update[i].level[i] -> node.level[i] -> update[i].level[i].forward
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// 8. 更新lv之上的索引层的span
	// 虽然新节点并没有插入这一层，但是寻找节点的时候是从最上层往下开始寻找的
	// 由于新结点没有插入这一层，所以从最高层只需向下走一步就可以找到该节点
	for i := lv; i < list.level; i++ {
		update[i].level[i].span++
	}

	// 9. 更新每层索引中被修改的节点的backward
	// 9.1 检查node是否为第一个节点，如果是就设置backward=nil
	node.backward = utils.If(update[0] == list.header, nil, update[0])
	// 9.2 检查node是否为最后一个节点
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node
	} else {
		list.tail = node
	}
	list.length++
	return node
}

// deleteNode 从链表中删除一个节点
//
// node: 要被删除的节点
//
// update: 要被删除的节点的之前一个节点
func (list *skiplist) deleteNode(node *skiplistNode, update []*skiplistNode) {
	// 1. 从底向上搜索
	for i := 0; i < list.level; i++ {
		// 1.1 如果update[i].level[i]的后继是node, 则进行删除操作, 同时更新span
		// 删除update[i].level[i]之后的节点会使update[i].level[i]指向的下一个节点有所变化
		// 删除的节点越多，距离下一个节点越远，所以加上 node.level[i].span 再减 1
		if update[i].level[i].forward == node {
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forward = node.level[i].forward
		} else { // 1.2 update[i].level[i]的后继不是node, 但删除node后少了一个节点, 所以span--
			update[i].level[i].span--
		}
	}
	// 2. 更新node的backward
	// 2.1 如果node.level[0]的forward不为nil, 更新backward
	if node.level[0].forward != nil {
		node.level[0].forward.backward = node.backward
	} else { // 2.2 如果node.level[0]的forward为nil说明它是最后一个节点
		list.tail = node.backward
	}
	// 3. 在删除的过程中有可能会删除某一层的所有节点导致那一层变为空
	// 需要修改list.level
	for list.level > 1 && list.header.level[list.level-1].forward == nil {
		list.level--
	}
	// 4. 表中节点个数减1
	list.length--
}

// delete 找到符合给定ele和score的节点, 然后删除
//
// 删除成功返回true, 失败返回false
func (list *skiplist) delete(ele string, score float64) bool {
	// 1. 过程同insert
	update := make([]*skiplistNode, skiplist_maxLevel)
	node := list.header
	for i := list.level - 1; i >= 0; i-- {
		for canForward(node.level[i], ele, score) {
			node = node.level[i].forward
		}
		update[i] = node
	}
	// 2. 因为有的元素的score值会重复，所以需要根据score和ele找到正确元素对象
	node = node.level[0].forward
	if node != nil && score == node.Score && node.Ele == ele {
		list.deleteNode(node, update)
		return true
	}
	return false
}

// getRank 根据传入的score和ele找到匹配的节点，返回其排名
//
// 没有匹配到节点返回0；排名从1开始，因为header的rank为0
func (list *skiplist) getRank(ele string, score float64) int64 {
	// 1. 过程同insert
	rank := int64(0)
	node := list.header
	for i := list.level - 1; i >= 0; i-- {
		for canForward(node.level[i], ele, score) {
			rank += node.level[i].span
			node = node.level[i].forward
		}
		// 1.1 因为找到的是目标节点的之前一个节点, 所以要再进行一组操作
		if node.level[i].forward != nil {
			rank += node.level[i].span
			node = node.level[i].forward
		}
		// 1.2 x可能指向header, 所以要检查
		if node.Score == score && node.Ele == ele {
			return rank
		}
	}
	// 2. 没找到, 返回0
	return 0
}

// getElementByRank 根据rank获取节点, rank以1为基础
//
// rank: 从1开始的int64整数
//
// 找到节点则返回, 否则返回nil
func (list *skiplist) getElementByRank(rank int64) *skiplistNode {
	curRank := int64(0)
	node := list.header
	for i := list.level - 1; i >= 0; i-- {
		// 如果rank为0, 当curRank = 0, node.level[i].span = 0时会返回header
		for node.level[i].forward != nil && (curRank+node.level[i].span) <= rank {
			curRank += node.level[i].span
			node = node.level[i].forward
		}
		if curRank == rank {
			return node
		}
	}
	return nil
}

// updateScore 更新一个节点的score
//
// 如果score修改之后引起位置的变化需要先删除后重新插入，反之直接修改即可
//
// 返回更新后的节点, 可能是新插入的节点, 也可能是原来存在的节点
func (list *skiplist) updateScore(ele string, score, newScore float64) *skiplistNode {
	update := make([]*skiplistNode, skiplist_maxLevel)
	node := list.header
	for i := list.level - 1; i >= 0; i-- {
		for canForward(node.level[i], ele, score) {
			node = node.level[i].forward
		}
		update[i] = node
	}
	node = node.level[0].forward
	// 1. 如果没有匹配到节点则返回nil
	if node == nil || node.Ele != ele || node.Score != score {
		return nil
	}
	// 2. 判断修改完score后位置是否发生改变
	// 2.1 没有改变直接修改
	if (node.backward == nil || node.backward.Score < newScore) &&
		(node.level[0].forward == nil || node.level[0].forward.Score > newScore) {
		node.Score = newScore
		return node
	}
	// 2.2 发生改变则重新插入
	list.deleteNode(node, update)
	return list.insert(node.Ele, newScore)
}

// isValidRange 判断从min到max是否在list的数值合法范围内
//
// 如果min超过尾节点tail, 返回false
//
// 如果max小于第一个节点, 返回false
func (list *skiplist) isValidRange(min, max Border) bool {
	// 1. 判断min和max是否有交集
	if min.isNotIntersected(max) {
		return false
	}

	// 2. 判断min超过尾节点tail
	node := list.tail
	if node == nil || !min.less(&node.Element) {
		return false
	}
	// 3. 判断max小于第一个节点
	node = list.header.level[0].forward
	if node == nil || !max.greater(&node.Element) {
		return false
	}
	return true
}

// getFirstInRange 获取从min到max范围的第一个元素
func (list *skiplist) getFirstInRange(min, max Border) *skiplistNode {
	// 1. 检查范围合法性
	if !list.isValidRange(min, max) {
		return nil
	}
	// 2. 找到范围内第一个元素的前一个元素
	node := list.header
	for i := list.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && !min.less(&node.level[i].forward.Element) {
			node = node.level[i].forward
		}
	}
	// 3. 向后移动获得结果
	node = node.level[0].forward
	// 4. 判断结果合法性
	if !max.greater(&node.Element) {
		return nil
	}
	return node
}

// getLastInRange 获取从min到max范围的最后一个元素
func (list *skiplist) getLastInRange(min, max Border) *skiplistNode {
	// 1. 检查范围合法性
	if !list.isValidRange(min, max) {
		return nil
	}
	// 2. 找到范围内的最后一个元素
	node := list.header
	for i := list.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && max.greater(&node.level[i].forward.Element) {
			node = node.level[i].forward
		}
	}
	// 3. 检查结果合法性
	if !min.less(&node.Element) {
		return nil
	}
	return node
}

// RemoveRange 删除从min到max范围内的元素, 可以限定数量
//
// limit: 限制数量
//
// 返回被删除的元素数组
func (list *skiplist) deleteRange(min, max Border, limit int) (removed []*Element) {
	if !list.isValidRange(min, max) {
		return nil
	}
	update := make([]*skiplistNode, skiplist_maxLevel)
	removed = []*Element{}
	node := list.header
	// 1. 查找范围内所有元素, 记录它们的前一个节点用于删除
	for i := list.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && !min.less(&node.level[i].forward.Element) {
			node = node.level[i].forward
		}
		update[i] = node
	}

	// 2. node向后移动获得范围内的第一个元素
	node = node.level[0].forward

	// 3. 删除范围内的所有元素
	for node != nil {
		// 3.1 如果当前元素大于max, 退出循环
		if !max.greater(&node.Element) {
			break
		}
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		list.deleteNode(node, update)
		// 3.2 检查限定数量
		if limit > 0 && len(removed) == limit {
			break
		}
		node = next
	}
	return removed
}

// deleteRangeByRank 根据rank范围删除, 左闭右闭
//
// start, stop: 从1开始的rank
//
// 返回被删除的元素数组
func (list *skiplist) deleteRangeByRank(start, stop int64) (removed []*Element) {
	rank := int64(0)
	update := make([]*skiplistNode, skiplist_maxLevel)
	removed = []*Element{}

	// 1. 更新update数组
	node := list.header
	for i := list.level - 1; i >= 0; i-- {
		for node.level[i].forward != nil && (rank+node.level[i].span) < start {
			rank += node.level[i].span
			node = node.level[i].forward
		}
		update[i] = node
	}

	rank++
	node = node.level[0].forward

	// 2. 删除
	for node != nil && rank <= stop {
		next := node.level[0].forward
		removedElement := node.Element
		removed = append(removed, &removedElement)
		list.deleteNode(node, update)
		node = next
		rank++
	}
	return removed
}

// canForward 判断寻找节点的循环能否继续, 此函数会找到给定的节点之前的一个节点
//
// l: 某一层的索引
//
// ele, score: 节点数值
//
// 能进行返回true, 否则返回false
func canForward(l *level, ele string, score float64) bool {
	return l.forward != nil &&
		(l.forward.Score < score ||
			(l.forward.Score == score && l.forward.Ele < ele))
}
