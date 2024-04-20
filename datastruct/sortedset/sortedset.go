package sortedset

import (
	"strconv"

	"go-redis/lib/logger"
)

type SortedSet struct {
	dict     map[string]*Element // 保存ele到score的映射
	skiplist *skiplist           // 保存score到ele的映射
}

func NewSortedSet() *SortedSet {
	return &SortedSet{
		dict:     make(map[string]*Element),
		skiplist: newSkiplist(),
	}
}

// Add 向sortedset中添加一个新的元素. 如果ele已经存在, 则更新score; 如果不存在, 则插入
//
// 如果ele原本不存在, 返回true; 否则返回false
func (set *SortedSet) Add(ele string, score float64) bool {
	// 1. 取出元素
	e, exist := set.dict[ele]
	// 2. 修改dict
	set.dict[ele] = &Element{
		ele,
		score,
	}
	// 3. 判断ele原本是否存在
	if exist {
		// 3.1 如果ele原本存在, 再判断score是否与原本一样, 为了去重
		if score != e.Score {
			// 3.1.1 score不一样则插入到skiplist中
			set.skiplist.updateScore(e.Ele, e.Score, score)
		}
		return false
	}
	// 4. ele不存在, 则新插入
	set.skiplist.insert(ele, score)
	return true
}

func (set *SortedSet) AddElement(e *Element) bool {
	return set.Add(e.Ele, e.Score)
}

// Length 获取sortedset的大小
func (set *SortedSet) Length() int64 {
	return set.skiplist.length
}

// Get 根据ele获取元素
func (set *SortedSet) Get(ele string) (*Element, bool) {
	e, exist := set.dict[ele]
	if !exist {
		return nil, false
	}
	return e, true
}

// Remove 根据ele删除元素
func (set *SortedSet) Remove(ele string) bool {
	e, exist := set.dict[ele]
	if exist {
		set.skiplist.delete(e.Ele, e.Score)
		delete(set.dict, e.Ele)
		return true
	}
	return false
}

// GetRank 根据ele获取rank, rank从0开始, 默认是升序
//
// ele: 元素的键, desc: 是否使用逆序排位
//
// 如果ele不存在返回-1, 否则返回rank
func (set *SortedSet) GetRank(ele string, desc bool) (rank int64) {
	e, exist := set.dict[ele]
	if !exist {
		return -1
	}
	rank = set.skiplist.getRank(e.Ele, e.Score)
	if desc {
		rank = set.skiplist.length - rank
	} else {
		rank--
	}
	return
}

// ForEachByRank 遍历[start, stop)内的元素, 执行consumer处理函数, rank从0开始
//
// start, stop: 从0开始的rank
//
// desc: 是否逆序排位
//
// consumer: 处理每个元素的函数
func (set *SortedSet) ForEachByRank(start, stop int64, desc bool, consumer func(e *Element) bool) {
	// 1. 边界合法性判断
	size := set.Length()
	if start < 0 || start >= size {
		logger.Fatal("illegal start " + strconv.FormatInt(start, 10))
	}
	if stop < start || stop > size {
		logger.Fatal("illegal end " + strconv.FormatInt(stop, 10))
	}

	// 2. 找到范围内的第一个元素
	var node *skiplistNode
	if desc { // 2.1 如果是逆序, 那么skiplist中的最后一个元素是要处理的第一个元素
		node = set.skiplist.tail
		if start > 0 {
			node = set.skiplist.getElementByRank(size - start)
		}
	} else {
		node = set.skiplist.header.level[0].forward
		if start > 0 {
			node = set.skiplist.getElementByRank(start + 1)
		}
	}

	// 3. 对[start, stop)范围内的元素做处理
	sliceSize := int(stop - start)
	for i := 0; i < sliceSize; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc { // 3.1 如果是逆序, 那么向backward的方向遍历
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
}

// RangeByRank 返回[start, stop)范围内的元素
func (set *SortedSet) RangeByRank(start, stop int64, desc bool) []*Element {
	sliceSize := int(stop - start)
	slice := make([]*Element, 0, sliceSize)
	set.ForEachByRank(start, stop, desc, func(e *Element) bool {
		slice = append(slice, e)
		return true
	})
	return slice
}

// RangeCount 范围从min到max范围内的元素的数量
func (set *SortedSet) RangeCount(min, max Border) int64 {
	count := int64(0)
	set.ForEachByRank(0, set.Length(), false, func(element *Element) bool {
		// 1. 如果min > element, 那么跳过计数继续循环
		if !min.less(element) {
			return true
		}
		// 2. 如果max < element, 说明超出范围了, 退出循环
		if !max.greater(element) {
			return false
		}
		// 3. min < element < max, 计数 + 1
		count++
		return true
	})
	return count
}

// ForEach 遍历从min到max内的元素, 执行consumer处理函数
func (set *SortedSet) ForEach(min, max Border, offset, limit int64, desc bool, consumer func(element *Element) bool) {
	// 1. 找到范围内的第一个元素
	var node *skiplistNode
	if desc {
		node = set.skiplist.getLastInRange(min, max)
	} else {
		node = set.skiplist.getFirstInRange(min, max)
	}
	for node != nil && offset > 0 {
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		offset--
	}

	for i := 0; (i < int(limit) || limit < 0) && node != nil; i++ {
		if !consumer(&node.Element) {
			break
		}
		if desc {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
		if node == nil {
			break
		}
		if !min.less(&node.Element) || !max.greater(&node.Element) {
			break
		}
	}
}

func (set *SortedSet) Range(min, max Border, offset, limit int64, desc bool) []*Element {
	if limit == 0 || offset < 0 {
		return make([]*Element, 0)
	}
	slice := make([]*Element, 0)
	set.ForEach(min, max, offset, limit, desc, func(element *Element) bool {
		slice = append(slice, element)
		return true
	})
	return slice
}

func (set *SortedSet) RemoveRange(min Border, max Border) int64 {
	removed := set.skiplist.deleteRange(min, max, 0)
	for _, element := range removed {
		delete(set.dict, element.Ele)
	}
	return int64(len(removed))
}

// PopMin 删除并返回有序集合key中的根据分数从小到大排名前count个成员
func (set *SortedSet) PopMin(count int) []*Element {
	first := set.skiplist.getFirstInRange(scoreNegativeInfBorder, scorePositiveInfBorder)
	if first == nil {
		return nil
	}
	border := &ScoreBorder{
		value:   first.Score,
		exclude: false,
	}
	removed := set.skiplist.deleteRange(border, scorePositiveInfBorder, count)
	for _, element := range removed {
		delete(set.dict, element.Ele)
	}
	return removed
}

// PopMax 删除并返回有序集合key中的根据分数从大到小排名前count个成员
func (set *SortedSet) PopMax(count int) []*Element {
	removed := make([]*Element, 0, count)
	set.ForEachByRank(0, int64(count), true, func(element *Element) bool {
		set.Remove(element.Ele)
		removedElement := *element
		removed = append(removed, &removedElement)
		return true
	})
	return removed
}

// RemoveByRank 移除有序集key中，指定排名(rank)区间内的所有成员（包含两端）
func (set *SortedSet) RemoveByRank(start, stop int64) int64 {
	removed := set.skiplist.deleteRangeByRank(start+1, stop+1)
	for _, element := range removed {
		delete(set.dict, element.Ele)
	}
	return int64(len(removed))
}
