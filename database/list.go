package database

import (
	"bytes"
	"strconv"
	"strings"

	"go-redis/datastruct/list"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

func (d *DB) getList(key string) (list.List, resp.ErrorReply) {
	entity, ok := d.getEntity(key)
	if !ok {
		return nil, nil
	}
	l, ok := entity.Data.(list.List)
	if !ok {
		return nil, reply.NewWrongTypeErrReply()
	}
	return l, nil
}

func (d *DB) getOrCreateList(key string) (l list.List, created bool, errReply resp.ErrorReply) {
	l, errReply = d.getList(key)
	if errReply != nil {
		return nil, false, errReply
	}
	created = false
	if l == nil {
		l = list.NewQuickList()
		d.putEntity(key, db.NewDataEntity(l))
		created = true
	}
	return l, created, nil
}

// execLIndex 命令用于通过索引获取列表中的元素。你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
//
// # LINDEX KEY_NAME INDEX_POSITION
//
// 返回: 列表中下标为指定索引值的元素。 如果指定索引值不在列表的区间范围内，返回 nil 。
func execLIndex(d *DB, args db.Params) resp.Reply {
	// 1. 获取参数
	key := utils.Bytes2String(args[0])
	index, err := strconv.ParseInt(utils.Bytes2String(args[1]), 10, 64)
	if err != nil {
		return reply.NewIntErrReply()
	}

	// 2. 获取list实例
	l, errReply := d.getList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return reply.NewNullBulkReply()
	}

	// 3. 计算index
	size := int64(l.Len())
	utils.Assert(size > 0)
	if index < -1*size {
		return reply.NewNullBulkReply()
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return reply.NewNullBulkReply()
	}

	// 4. 获取数据
	val, _ := l.Get(int(index)).([]byte)
	return reply.NewBulkReply(val)
}

// execLLen 命令用于返回列表的长度。 如果列表 key 不存在，则 key 被解释为一个空列表，返回 0 。 如果 key 不是列表类型，返回一个错误。
//
// # LLEN KEY_NAME
//
// 返回: 列表的长度。
func execLLen(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])

	l, errReply := d.getList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return reply.NewIntReply(0)
	}
	return reply.NewIntReply(int64(l.Len()))
}

// execLPop 命令用于移除并返回列表的第一个元素。
//
// # LPOP KEY_NAME
//
// 列表的第一个元素。 当列表 key 不存在时，返回 nil 。
func execLPop(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])

	l, errReply := d.getList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return reply.NewNullBulkReply()
	}

	val, _ := l.Remove(0).([]byte)
	if l.Len() == 0 {
		d.Remove(key)
	}
	d.append(utils.ToCmdLine2(enum.LPOP.String(), args...))
	return reply.NewBulkReply(val)
}

func undoLPop(d *DB, args db.Params) []db.CmdLine {
	key := utils.Bytes2String(args[0])
	l, errReply := d.getList(key)
	if errReply != nil {
		return nil
	}
	if l == nil || l.Len() == 0 {
		return nil
	}
	element, _ := l.Get(0).([]byte)

	return []db.CmdLine{utils.ToCmdLine2(enum.LPUSH.String(), args[0], element)}
}

// execLPush 将一个或多个值插入到列表头部。
// 如果 key 不存在，一个空列表会被创建并执行 LPUSH 操作。 当 key 存在但不是列表类型时，返回一个错误。
//
// # LPUSH KEY_NAME VALUE1.. VALUEN
//
// 返回: 执行 LPUSH 命令后，列表的长度。
func execLPush(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	values := args[1:]

	l, _, errReply := d.getOrCreateList(key)
	if errReply != nil {
		return errReply
	}

	for _, value := range values {
		l.Insert(0, value)
	}

	d.append(utils.ToCmdLine2(enum.LPUSH.String(), args...))
	return reply.NewIntReply(int64(l.Len()))
}

func undoLPush(_ *DB, args db.Params) []db.CmdLine {
	key := utils.Bytes2String(args[0])
	count := len(args) - 1
	cmdLines := make([]db.CmdLine, 0, count)
	for i := 0; i < count; i++ {
		cmdLines = append(cmdLines, utils.ToCmdLine(enum.LPOP.String(), key))
	}
	return cmdLines
}

// execLPushX 将一个值插入到已存在的列表头部，列表不存在时操作无效。
//
// # LPUSHX KEY_NAME VALUE1.. VALUEN
//
// 返回: LPUSHX 命令执行之后，列表的长度。
func execLPushX(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	values := args[1:]

	l, errReply := d.getList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return reply.NewIntReply(0)
	}

	for _, value := range values {
		l.Insert(0, value)
	}
	d.append(utils.ToCmdLine2(enum.LPUSHX.String(), args...))
	return reply.NewIntReply(int64(l.Len()))
}

// execLRange 返回列表中指定区间内的元素，区间以偏移量 START 和 END 指定。 其中 0 表示列表的第一个元素， 1 表示列表的第二个元素，以此类推。
// 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
//
// # LRANGE KEY_NAME START END
//
// 返回: 一个列表，包含指定区间内的元素。
func execLRange(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	start, err := strconv.Atoi(utils.Bytes2String(args[1]))
	if err != nil {
		return reply.NewIntErrReply()
	}
	stop, err := strconv.Atoi(utils.Bytes2String(args[2]))
	if err != nil {
		return reply.NewIntErrReply()
	}

	l, errReply := d.getList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return reply.NewEmptyMultiBulkReply()
	}

	err = computeInterval(l.Len(), &start, &stop)
	if err != nil {
		return reply.NewEmptyMultiBulkReply()
	}

	// assert: start in [0, size - 1], stop in [start, size]
	slice := l.Range(start, stop)
	result := make([][]byte, len(slice))
	for i, raw := range slice {
		result[i] = raw.([]byte)
	}
	return reply.NewMultiBulkReply(result)
}

// execLRem 根据参数 COUNT 的值，移除列表中与参数 VALUE 相等的元素。
//
// COUNT 的值可以是以下几种：
//
// (1)count > 0 : 从表头开始向表尾搜索，移除与 VALUE 相等的元素，数量为 COUNT 。
//
// (2)count < 0 : 从表尾开始向表头搜索，移除与 VALUE 相等的元素，数量为 COUNT 的绝对值。
//
// (3)count = 0 : 移除表中所有与 VALUE 相等的值。
//
// # LREM key count VALUE
//
// 返回: 被移除元素的数量。 列表不存在时返回 0 。
func execLRem(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	count, err := strconv.Atoi(utils.Bytes2String(args[1]))
	if err != nil {
		return reply.NewIntErrReply()
	}
	value := args[2]

	l, errReply := d.getList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return reply.NewIntReply(0)
	}

	var removed int
	if count == 0 { // 移除表中所有与 VALUE 相等的值。
		removed = l.RemoveAllByVal(func(a any) bool {
			return utils.Equals(a, value)
		})
	} else if count > 0 { // 从表头开始向表尾搜索，移除与 VALUE 相等的元素，数量为 COUNT
		removed = l.RemoveByVal(func(a any) bool {
			return utils.Equals(a, value)
		}, count)
	} else { // 从表尾开始向表头搜索，移除与 VALUE 相等的元素，数量为 COUNT 的绝对值
		removed = l.ReverseRemoveByVal(func(a any) bool {
			return utils.Equals(a, value)
		}, -count)
	}

	if l.Len() == 0 {
		d.Remove(key)
	}
	if removed > 0 {
		d.append(utils.ToCmdLine2(enum.LREM.String(), args...))
	}

	return reply.NewIntReply(int64(removed))
}

// execLSet 通过索引来设置元素的值。
//
// 当索引参数超出范围，或对一个空列表进行 LSET 时，返回一个错误。
//
// # LSET KEY_NAME INDEX VALUE
//
// 操作成功返回 ok ，否则返回错误信息。
func execLSet(d *DB, args db.Params) resp.Reply {
	// parse args
	key := utils.Bytes2String(args[0])
	index, err := strconv.Atoi(utils.Bytes2String(args[1]))
	if err != nil {
		return reply.NewIntErrReply()
	}
	value := args[2]

	// get data
	l, errReply := d.getList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return reply.NewNoSuchKeyErrReply()
	}

	size := l.Len()
	utils.Assert(size > 0)
	if index < -1*size {
		return reply.NewErrReplyByError(enum.INDEX_OUT_OF_RANGE)
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return reply.NewErrReplyByError(enum.INDEX_OUT_OF_RANGE)
	}

	l.Set(index, value)
	d.append(utils.ToCmdLine2(enum.LSET.String(), args...))
	return reply.NewOKReply()
}

func undoLSet(d *DB, args db.Params) []db.CmdLine {
	key := utils.Bytes2String(args[0])
	index64, err := strconv.ParseInt(utils.Bytes2String(args[1]), 10, 64)
	if err != nil {
		return nil
	}
	index := int(index64)
	l, errReply := d.getList(key)
	if errReply != nil {
		return nil
	}
	if l == nil {
		return nil
	}

	size := l.Len()
	utils.Assert(size > 0)

	if index < -1*size {
		return nil
	} else if index < 0 {
		index = size + index
	} else if index >= size {
		return nil
	}

	value, _ := l.Get(index).([]byte)

	return []db.CmdLine{utils.ToCmdLine2(enum.LSET.String(), args[0], args[1], value)}
}

// execRPop  命令用于移除列表的最后一个元素，返回值为移除的元素。
//
// # RPOP KEY_NAME
//
// 返回: 被移除的元素。 当列表不存在时，返回 nil 。
func execRPop(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])

	l, errReply := d.getList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return reply.NewNullBulkReply()
	}

	val, _ := l.RemoveLast().([]byte)
	if l.Len() == 0 {
		d.Remove(key)
	}
	d.append(utils.ToCmdLine2(enum.RPOP.String(), args...))
	return reply.NewBulkReply(val)
}

func undoRPop(d *DB, args db.Params) []db.CmdLine {
	key := utils.Bytes2String(args[0])
	l, errReply := d.getList(key)
	if errReply != nil {
		return nil
	}
	if l == nil || l.Len() == 0 {
		return nil
	}
	element := l.Get(l.Len() - 1).([]byte)

	return []db.CmdLine{utils.ToCmdLine2(enum.LPUSH.String(), args[0], element)}
}

func prepareRPopLPush(args db.Params) (writeKeys []string, readKeys []string) {
	return []string{
		utils.Bytes2String(args[0]),
		utils.Bytes2String(args[1]),
	}, nil
}

// execRPopLPush 命令用于移除列表的最后一个元素，并将该元素添加到另一个列表的头部并返回。
//
// # RPOPLPUSH SOURCE_KEY_NAME DESTINATION_KEY_NAME
//
// 返回: 被弹出的元素。
func execRPopLPush(d *DB, args db.Params) resp.Reply {
	sourceKey := utils.Bytes2String(args[0])
	destKey := utils.Bytes2String(args[1])

	sourceList, errReply := d.getList(sourceKey)
	if errReply != nil {
		return errReply
	}
	if sourceList == nil {
		return reply.NewNullBulkReply()
	}

	destList, _, errReply := d.getOrCreateList(destKey)
	if errReply != nil {
		return errReply
	}

	val, _ := sourceList.RemoveLast().([]byte)
	destList.Insert(0, val)

	if sourceList.Len() == 0 {
		d.Remove(sourceKey)
	}

	d.append(utils.ToCmdLine2(enum.RPOPLPUSH.String(), args...))
	return reply.NewBulkReply(val)
}

func undoRPopLPush(d *DB, args db.Params) []db.CmdLine {
	sourceKey := utils.Bytes2String(args[0])
	l, errReply := d.getList(sourceKey)
	if errReply != nil {
		return nil
	}
	if l == nil || l.Len() == 0 {
		return nil
	}
	element, _ := l.Get(l.Len() - 1).([]byte)
	return []db.CmdLine{
		{
			enum.RPUSH.Bytes(),
			args[0],
			element,
		},
		{
			enum.LPOP.Bytes(),
			args[1],
		},
	}
}

// execRPush 命令用于将一个或多个值插入到列表的尾部(最右边)。
//
// # RPUSH KEY_NAME VALUE1..VALUEN
//
// 执行 RPUSH 操作后，列表的长度。
func execRPush(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	values := args[1:]

	l, _, errReply := d.getOrCreateList(key)
	if errReply != nil {
		return errReply
	}

	for _, value := range values {
		l.PushBack(value)
	}
	d.append(utils.ToCmdLine2(enum.RPUSH.String(), args...))
	return reply.NewIntReply(int64(l.Len()))
}

func undoRPush(_ *DB, args db.Params) []db.CmdLine {
	key := utils.Bytes2String(args[0])
	count := len(args) - 1
	cmdLines := make([]db.CmdLine, 0, count)
	for i := 0; i < count; i++ {
		cmdLines = append(cmdLines, utils.ToCmdLine(enum.RPOP.String(), key))
	}
	return cmdLines
}

// execRPushX 命令用于将一个值插入到已存在的列表尾部(最右边)。如果列表不存在，操作无效。
//
// # RPUSHX KEY_NAME VALUE1..VALUEN
//
// 返回: 执行 Rpushx 操作后，列表的长度。
func execRPushX(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	values := args[1:]

	l, errReply := d.getList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return reply.NewIntReply(0)
	}

	for _, value := range values {
		l.PushBack(value)
	}
	d.append(utils.ToCmdLine2(enum.RPUSHX.String(), args...))

	return reply.NewIntReply(int64(l.Len()))
}

// execLTrim 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
//
// 下标 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
//
// # LTRIM KEY_NAME START STOP
//
// 返回: 命令执行成功时，返回 ok
func execLTrim(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	start, stop, errorReply := getInterval(args)
	if errorReply != nil {
		return errorReply
	}

	l, errReply := d.getList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return reply.NewOKReply()
	}

	length := int64(l.Len())
	if start < 0 {
		start += length
	}
	if stop < 0 {
		stop += length
	}

	leftCount := start
	rightCount := length - stop - 1

	for i := int64(0); i < leftCount && l.Len() > 0; i++ {
		l.Remove(0)
	}
	for i := int64(0); i < rightCount && l.Len() > 0; i++ {
		l.RemoveLast()
	}

	d.append(utils.ToCmdLine2(enum.LTRIM.String(), args...))

	return reply.NewOKReply()
}

// 命令用于在列表的元素前或者后插入元素。当指定元素不存在于列表中时，不执行任何操作。
//
// 当列表不存在时，被视为空列表，不执行任何操作。
//
// 如果 key 不是列表类型，返回一个错误。
//
// # LINSERT key BEFORE|AFTER pivot value
//
// 如果命令执行成功，返回插入操作完成之后，列表的长度。 如果没有找到指定元素 ，返回 -1 。 如果 key 不存在或为空列表，返回 0 。
func execLInsert(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	l, errReply := d.getList(key)
	if errReply != nil {
		return errReply
	}
	if l == nil {
		return reply.NewIntReply(0)
	}

	dir := strings.ToUpper(utils.Bytes2String(args[1]))
	if dir != enum.LIST_BEFORE && dir != enum.LIST_AFTER {
		return reply.NewSyntaxErrReply()
	}

	pivot := args[2]
	index := -1
	l.ForEach(func(i int, v any) bool {
		if bytes.Equal(pivot, v.([]byte)) {
			index = i
			return false
		}
		return true
	})
	if index == -1 {
		return reply.NewIntReply(-1)
	}

	val := args[3]
	if dir == enum.LIST_BEFORE {
		l.Insert(index, val)
	} else {
		l.Insert(index+1, val)
	}

	d.append(utils.ToCmdLine2(enum.LINSERT.String(), args...))

	return reply.NewIntReply(int64(l.Len()))
}

func init() {
	registerCommand(enum.LINDEX, readFirstKey, execLIndex, nil)                     //
	registerCommand(enum.LLEN, readFirstKey, execLLen, nil)                         //
	registerCommand(enum.LPOP, writeFirstKey, execLPop, undoLPop)                   //
	registerCommand(enum.LPUSH, writeFirstKey, execLPush, undoLPush)                //
	registerCommand(enum.LPUSHX, writeFirstKey, execLPushX, undoLPush)              //
	registerCommand(enum.LRANGE, readFirstKey, execLRange, nil)                     //
	registerCommand(enum.LREM, writeFirstKey, execLRem, rollbackFirstKey)           //
	registerCommand(enum.LSET, writeFirstKey, execLSet, undoLSet)                   //
	registerCommand(enum.RPOP, writeFirstKey, execRPop, undoRPop)                   //
	registerCommand(enum.RPOPLPUSH, prepareRPopLPush, execRPopLPush, undoRPopLPush) //
	registerCommand(enum.RPUSH, writeFirstKey, execRPush, undoRPush)                //
	registerCommand(enum.RPUSHX, writeFirstKey, execRPushX, undoRPush)              //
	registerCommand(enum.LTRIM, writeFirstKey, execLTrim, rollbackFirstKey)         //
	registerCommand(enum.LINSERT, writeFirstKey, execLInsert, rollbackFirstKey)
}
