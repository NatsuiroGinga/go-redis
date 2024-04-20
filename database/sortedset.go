package database

import (
	"strconv"
	"strings"

	"go-redis/datastruct/sortedset"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// execZAdd 命令用于将一个或多个成员元素及其分数值加入到有序集当中。
//
// 如果某个成员已经是有序集的成员，那么更新这个成员的分数值，并通过重新插入这个成员元素，来保证该成员在正确的位置上。
//
// 分数值可以是整数值或双精度浮点数。
//
// 如果有序集合 key 不存在，则创建一个空的有序集并执行 ZADD 操作。
//
// 当 key 存在但不是有序集类型时，返回一个错误。
//
// # ZADD KEY_NAME SCORE1 VALUE1.. SCOREN VALUEN
//
// 被成功添加的新成员的数量，不包括那些被更新的、已经存在的成员。
func execZAdd(d *DB, args db.Params) resp.Reply {
	// 1. 参数必须是奇数个
	if len(args)%2 != 1 {
		return reply.NewSyntaxErrReply()
	}
	// 2. 获取key和key-value数量
	key := utils.Bytes2String(args[0])
	size := (len(args) - 1) / 2
	// 3. 根据参数创建要插入的元素数组
	elements := make([]*sortedset.Element, size)
	for i := 0; i < size; i++ {
		scoreValue := args[2*i+1]
		ele := utils.Bytes2String(args[2*i+2])
		score, err := strconv.ParseFloat(utils.Bytes2String(scoreValue), 64)
		if err != nil {
			return reply.NewNotValidFloatErrReply()
		}
		elements[i] = &sortedset.Element{
			Ele:   ele,
			Score: score,
		}
	}

	// 4. 取出或创建sortedset
	sortedSet, _, errReply := d.getOrCreateSortedSet(key)
	if errReply != nil {
		return errReply
	}

	// 5. 添加元素并计数
	i := int64(0)
	for _, e := range elements {
		if sortedSet.AddElement(e) {
			i++
		}
	}

	d.append(utils.ToCmdLine2(enum.ZADD.Name(), args...))

	return reply.NewIntReply(i)
}

// execZScore 命令返回有序集中，成员的分数值。 如果成员元素不是有序集 key 的成员，或 key 不存在，返回 nil 。
//
// # ZSCORE key member
//
// 成员的分数值，以字符串形式表示。
func execZScore(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	ele := utils.Bytes2String(args[1])

	sortedSet, errReply := d.getSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return reply.NewNullBulkReply()
	}

	element, exists := sortedSet.Get(ele)
	if !exists {
		return reply.NewNullBulkReply()
	}
	value := strconv.FormatFloat(element.Score, 'f', -1, 64)
	return reply.NewBulkReply(utils.String2Bytes(value))
}

// execZRank 返回有序集中指定成员的排名。其中有序集成员按分数值递增(从小到大)顺序排列。
//
// # ZRANK key member
//
// 如果成员是有序集 key 的成员，返回 member 的排名。 如果成员不是有序集 key 的成员，返回 nil 。
func execZRank(d *DB, args db.Params) resp.Reply {
	return execGenericZRank(d, args, enum.ZREVRANK)
}

// execZRevRank 命令返回有序集中成员的排名。其中有序集成员按分数值递减(从大到小)排序。
//
// # ZREVRANK key member
//
// 排名以 0 为底，也就是说， 分数值最大的成员排名为 0 。
//
// 如果成员是有序集 key 的成员，返回成员的排名。 如果成员不是有序集 key 的成员，返回 nil 。
func execZRevRank(d *DB, args db.Params) resp.Reply {
	return execGenericZRank(d, args, enum.ZREVRANK)
}

func execGenericZRank(d *DB, args db.Params, cmd *enum.Command) resp.Reply {
	// parse args
	key := utils.Bytes2String(args[0])
	ele := utils.Bytes2String(args[1])

	// get entity
	sortedSet, errReply := d.getSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return reply.NewNullBulkReply()
	}

	rank := sortedSet.GetRank(ele, utils.If(cmd == enum.ZRANK, false, true))
	if rank < 0 {
		return reply.NewNullBulkReply()
	}
	return reply.NewIntReply(rank)
}

// execZCard 命令用于计算集合中元素的数量。
//
// # ZCARD KEY_NAME
//
// 当 key 存在且是有序集类型时，返回有序集的基数。 当 key 不存在时，返回 0 。
func execZCard(d *DB, args db.Params) resp.Reply {
	// parse args
	key := utils.Bytes2String(args[0])

	// get entity
	set, errReply := d.getSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return reply.NewIntReply(0)
	}

	return reply.NewIntReply(set.Length())
}

// execZCount 命令在计算有序集合中指定字典区间内成员数量。
//
// # ZLEXCOUNT KEY MIN MAX
//
// 返回指定区间内的成员数量
func execZCount(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])

	minBorder, maxBorder, errorReply := getBorders(args)
	if errorReply != nil {
		return errorReply
	}

	// get data
	sortedSet, errReply := d.getSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return reply.NewIntReply(0)
	}

	return reply.NewIntReply(sortedSet.RangeCount(minBorder, maxBorder))
}

// execZRange gets members in range, sort by score in ascending order
func execZRange(d *DB, args db.Params) resp.Reply {
	return execGenericZRangeCommand(d, args, enum.ZRANGE)
}

// execZRevRange gets members in range, sort by score in descending order
func execZRevRange(d *DB, args db.Params) resp.Reply {
	return execGenericZRangeCommand(d, args, enum.ZREVRANGE)
}

// execGenericZRangeCommand 实现ZRange和ZRevRange命令
func execGenericZRangeCommand(d *DB, args db.Params, cmd *enum.Command) resp.Reply {
	if len(args) != 3 && len(args) != 4 {
		return reply.NewArgNumErrReply(cmd.String())
	}
	withScores := false
	if len(args) == 4 {
		if utils.Bytes2String(args[3]) != enum.WITH_SCORES {
			return reply.NewSyntaxErrReply()
		}
		withScores = true
	}
	key := utils.Bytes2String(args[0])
	start, stop, errorReply := getInterval(args)
	if errorReply != nil {
		return errorReply
	}
	return range0(d, key, start, stop, withScores, utils.If(cmd == enum.ZRANGE, false, true))
}

// execZRangeByScore 返回有序集合中指定分数区间的成员列表。有序集成员按分数值递增(从小到大)次序排列。
//
// 具有相同分数值的成员按字典序来排列(该属性是有序集提供的，不需要额外的计算)。
//
// 默认情况下，区间的取值使用闭区间 (小于等于或大于等于)，你也可以通过给参数前增加 ( 符号来使用可选的开区间 (小于或大于)。
//
// 格式: ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
//
// 返回: 指定区间内，带有分数值(可选)的有序集成员的列表。
func execZRangeByScore(d *DB, args db.Params) resp.Reply {
	return execGenericZRangeByScoreCommand(d, args, enum.ZRANGEBYSCORE)
}

// execZRevRangeByScore 返回有序集中指定分数区间内的所有的成员。有序集成员按分数值递减(从大到小)的次序排列。
//
// 具有相同分数值的成员按字典序的逆序(reverse lexicographical order )排列。
//
// 除了成员按分数值递减的次序排列这一点外， ZREVRANGEBYSCORE 命令的其他方面和 ZRANGEBYSCORE 命令一样。
//
// 格式: ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
//
// 返回: 指定区间内，带有分数值(可选)的有序集成员的列表。
func execZRevRangeByScore(d *DB, args db.Params) resp.Reply {
	return execGenericZRangeByScoreCommand(d, args, enum.ZREVRANGEBYSCORE)
}

// execGenericZRangeByScoreCommand 实现了ZRangeByScore和ZRevRangeByScore命令
func execGenericZRangeByScoreCommand(d *DB, args db.Params, cmd *enum.Command) resp.Reply {
	if len(args) < 3 {
		return reply.NewArgNumErrReply(cmd.String())
	}
	key := utils.Bytes2String(args[0])

	var maxBorder, minBorder sortedset.Border
	var errorReply resp.Reply
	var reverse bool
	switch cmd {
	case enum.ZRANGEBYSCORE:
		minBorder, maxBorder, errorReply = getBorders(args)
		reverse = false
	case enum.ZREVRANGEBYSCORE:
		maxBorder, minBorder, errorReply = getBorders(args)
		reverse = true
	default:
		return reply.NewErrReply("unknown ZRangeByScore command")
	}

	if errorReply != nil {
		return errorReply
	}

	withScores, offset, limit, errReply := getOptionalParameters(args)
	if errReply != nil {
		return errorReply
	}

	return rangeByScore0(d, key, minBorder, maxBorder, offset, limit, withScores, reverse)
}

// execZRem 命令用于移除有序集中的一个或多个成员，不存在的成员将被忽略。
//
// 当 key 存在但不是有序集类型时，返回一个错误
func execZRem(d *DB, args db.Params) resp.Reply {
	// parse args
	key := utils.Bytes2String(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = utils.Bytes2String(v)
	}

	// get entity
	sortedSet, errReply := d.getSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return reply.NewIntReply(0)
	}

	var deleted int64 = 0
	for _, field := range fields {
		if sortedSet.Remove(field) {
			deleted++
		}
	}
	if deleted > 0 {
		d.append(utils.ToCmdLine2(enum.ZREM.String(), args...))
	}
	return reply.NewIntReply(deleted)
}

// execZRemRangeByScore removes members which score within given range
func execZRemRangeByScore(d *DB, args db.Params) resp.Reply {
	if len(args) != 3 {
		return reply.NewArgNumErrReply(enum.ZREMRANGEBYSCORE.String())
	}
	key := utils.Bytes2String(args[0])

	minBorder, maxBorder, errorReply := getBorders(args)
	if errorReply != nil {
		return errorReply
	}

	// get data
	sortedSet, errReply := d.getSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return reply.NewEmptyMultiBulkReply()
	}

	removed := sortedSet.RemoveRange(minBorder, maxBorder)
	if removed > 0 {
		d.append(utils.ToCmdLine2(enum.ZREMRANGEBYSCORE.String(), args...))
	}
	return reply.NewIntReply(removed)
}

// execZRemRangeByRank 命令用于移除有序集中，指定排名(rank)区间内的所有成员, 左闭右闭
//
// 格式: ZREMRANGEBYRANK key start stop
//
// 返回: 被移除成员的数量。
func execZRemRangeByRank(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	start, stop, errorReply := getInterval(args)
	if errorReply != nil {
		return errorReply
	}

	// get data
	sortedSet, errReply := d.getSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return reply.NewIntReply(0)
	}

	if err := computeInterval(sortedSet.Length(), &start, &stop); err != nil {
		return errorReply
	}

	// assert: start in [0, size - 1], stop in [start, size]
	removed := sortedSet.RemoveByRank(start, stop)
	if removed > 0 {
		d.append(utils.ToCmdLine2(enum.ZREMRANGEBYRANK.String(), args...))
	}
	return reply.NewIntReply(removed)
}

// execZPopMin 删除并返回有序集合key中的根据分数从小到大排名前count个成员
//
// 格式: ZPOPMIN KEY_NAME [COUNT]
//
// 返回: ele-scores集合
func execZPopMin(d *DB, args db.Params) resp.Reply {
	return execGenericZPopCommand(d, args, enum.ZPOPMIN)
}

func execZPopMax(d *DB, args db.Params) resp.Reply {
	return execGenericZPopCommand(d, args, enum.ZPOPMAX)
}

// execGenericZPopCommand 实现ZPopMin和ZPopMax
func execGenericZPopCommand(d *DB, args db.Params, cmd *enum.Command) resp.Reply {
	key := utils.Bytes2String(args[0])
	count := 1
	if len(args) > 1 {
		var err error
		count, err = strconv.Atoi(utils.Bytes2String(args[1]))
		if err != nil {
			return reply.NewIntErrReply()
		}
	}

	sortedSet, errReply := d.getSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return reply.NewEmptyMultiBulkReply()
	}

	var removed []*sortedset.Element

	switch cmd {
	case enum.ZPOPMIN:
		removed = sortedSet.PopMin(count)
	case enum.ZPOPMAX:
		removed = sortedSet.PopMax(count)
	default:
		return reply.NewErrReply("unknown ZPop command")
	}

	if len(removed) > 0 {
		d.append(utils.ToCmdLine2(cmd.String(), args...))
	}
	result := make([][]byte, 0, len(removed)*2)
	for _, element := range removed {
		scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
		result = append(result, utils.ToCmdLine(element.Ele, scoreStr)...)
	}
	return reply.NewMultiBulkReply(result)
}

// execZIncrBy 命令对有序集合中指定成员的分数加上增量 increment
//
// 可以通过传递一个负数值 increment ，让分数减去相应的值，比如 ZINCRBY key -5 member ，就是让 member 的 score 值减去 5 。
//
// 当 key 不存在，或分数不是 key 的成员时， ZINCRBY key increment member 等同于 ZADD key increment member 。
//
// 当 key 不是有序集类型时，返回一个错误。
//
// 分数值可以是整数值或双精度浮点数。
//
// # ZINCRBY key increment member
//
// 返回 member 成员的新分数值，以字符串形式表示。
func execZIncrBy(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	rawDelta := utils.Bytes2String(args[1])
	field := utils.Bytes2String(args[2])
	delta, err := strconv.ParseFloat(rawDelta, 64)
	if err != nil {
		return reply.NewNotValidFloatErrReply()
	}

	// get or init entity
	sortedSet, _, errReply := d.getOrCreateSortedSet(key)
	if errReply != nil {
		return errReply
	}

	element, exists := sortedSet.Get(field)
	if !exists {
		sortedSet.Add(field, delta)
		d.append(utils.ToCmdLine2(enum.ZINCRBY.String(), args...))
		return reply.NewBulkReply(args[1])
	}
	score := element.Score + delta
	sortedSet.Add(field, score)
	result := utils.String2Bytes(strconv.FormatFloat(score, 'f', -1, 64))
	d.append(utils.ToCmdLine2(enum.ZINCRBY.String(), args...))
	return reply.NewBulkReply(result)
}

func range0(d *DB, key string, start, stop int64, withScores, desc bool) resp.Reply {
	// get data
	sortedSet, errReply := d.getSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return reply.NewEmptyMultiBulkReply()
	}

	err := computeInterval(sortedSet.Length(), &start, &stop)
	if err != nil {
		return reply.NewEmptyMultiBulkReply()
	}

	// assert: start in [0, size - 1], stop in [start, size]
	slice := sortedSet.RangeByRank(start, stop, desc)
	if withScores {
		result := make([][]byte, len(slice)*2)
		i := 0
		for _, element := range slice {
			result[i] = utils.String2Bytes(element.Ele)
			i++
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = utils.String2Bytes(scoreStr)
			i++
		}
		return reply.NewMultiBulkReply(result)
	}
	result := make([][]byte, 0, len(slice))
	for _, element := range slice {
		result = append(result, utils.String2Bytes(element.Ele))
	}
	return reply.NewMultiBulkReply(result)
}

func rangeByScore0(d *DB, key string, min, max sortedset.Border, offset, limit int64, withScores, desc bool) resp.Reply {
	// get data
	sortedSet, errReply := d.getSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return reply.NewEmptyMultiBulkReply()
	}

	slice := sortedSet.Range(min, max, offset, limit, desc)
	if withScores {
		result := make([][]byte, len(slice)*2)
		i := 0
		for _, element := range slice {
			result[i] = utils.String2Bytes(element.Ele)
			i++
			scoreStr := strconv.FormatFloat(element.Score, 'f', -1, 64)
			result[i] = utils.String2Bytes(scoreStr)
			i++
		}
		return reply.NewMultiBulkReply(result)
	}
	result := make([][]byte, 0, len(slice))
	for _, element := range slice {
		result = append(result, utils.String2Bytes(element.Ele))
	}
	return reply.NewMultiBulkReply(result)
}

// getSortedSet 根据key获取dict中的sortedset
//
// 如果key不存在, 返回nil, nil;
// 如果key存在, 但类型不对, 返回ErrReply; 如果类型正确, 返回数据和nil
func (d *DB) getSortedSet(key string) (*sortedset.SortedSet, resp.ErrorReply) {
	entity, exists := d.getEntity(key)
	if !exists {
		return nil, nil
	}
	sortedSet, ok := entity.Data.(*sortedset.SortedSet)
	if !ok {
		return nil, reply.NewWrongTypeErrReply()
	}
	return sortedSet, nil
}

// getOrCreateSortedSet 根据key获取或者创建一个sortedset
//
// sortedset: 数据实体
//
// isCreate: 创建是否成功
//
// errReply: 错误回复
func (d *DB) getOrCreateSortedSet(key string) (sortedSet *sortedset.SortedSet, isCreate bool, errReply resp.ErrorReply) {
	sortedSet, errReply = d.getSortedSet(key)
	if errReply != nil {
		return nil, false, errReply
	}
	isCreate = false
	if sortedSet == nil {
		sortedSet = sortedset.NewSortedSet()
		d.putEntity(key, db.NewDataEntity(sortedSet))
		isCreate = true
	}
	return sortedSet, isCreate, nil
}

// getBorders 根据命令的参数获取两个边界
func getBorders(args db.Params) (border1, border2 sortedset.Border, errorReply resp.Reply) {
	var err error
	border1, err = sortedset.ParseScoreBorder(utils.Bytes2String(args[1]))
	if err != nil {
		return nil, nil, reply.NewErrReplyByError(err)
	}

	border2, err = sortedset.ParseScoreBorder(utils.Bytes2String(args[2]))
	if err != nil {
		return nil, nil, reply.NewErrReplyByError(err)
	}
	return
}

// getOptionalParameters 获取命令的参数中的可选参数, 例如: withscores, offset, limit
func getOptionalParameters(args db.Params) (withScores bool, offset, limit int64, errReply resp.ErrorReply) {
	var err error
	withScores = false
	offset = int64(0)
	limit = int64(-1)
	if len(args) > 3 {
		for i := 3; i < len(args); {
			s := utils.Bytes2String(args[i])
			switch strings.ToUpper(s) {
			case enum.WITH_SCORES:
				withScores = true
				i++
			case enum.LIMIT:
				if len(args) < i+3 {
					errReply = reply.NewSyntaxErrReply()
					return
				}
				offset, err = strconv.ParseInt(utils.Bytes2String(args[i+1]), 10, 64)
				if err != nil {
					errReply = reply.NewIntErrReply()
					return
				}
				limit, err = strconv.ParseInt(utils.Bytes2String(args[i+2]), 10, 64)
				if err != nil {
					errReply = reply.NewIntErrReply()
					return
				}
				i += 3
			default:
				errReply = reply.NewSyntaxErrReply()
				return
			}
		}
	}
	return
}

func init() {
	registerCommand(enum.ZADD, writeFirstKey, execZAdd)
	registerCommand(enum.ZSCORE, readFirstKey, execZScore)
	registerCommand(enum.ZINCRBY, writeFirstKey, execZIncrBy)
	registerCommand(enum.ZRANK, readFirstKey, execZRank)
	registerCommand(enum.ZCOUNT, readFirstKey, execZCount)
	registerCommand(enum.ZREVRANK, readFirstKey, execZRevRank)
	registerCommand(enum.ZCARD, readFirstKey, execZCard)
	registerCommand(enum.ZRANGE, readFirstKey, execZRange)
	registerCommand(enum.ZRANGEBYSCORE, readFirstKey, execZRangeByScore)
	registerCommand(enum.ZREVRANGE, readFirstKey, execZRevRange)
	registerCommand(enum.ZREVRANGEBYSCORE, readFirstKey, execZRevRangeByScore)
	registerCommand(enum.ZPOPMIN, writeFirstKey, execZPopMin)
	registerCommand(enum.ZPOPMAX, writeFirstKey, execZPopMax)
	registerCommand(enum.ZREM, writeFirstKey, execZRem)
	registerCommand(enum.ZREMRANGEBYSCORE, writeFirstKey, execZRemRangeByScore)
	registerCommand(enum.ZREMRANGEBYRANK, writeFirstKey, execZRemRangeByRank)
}
