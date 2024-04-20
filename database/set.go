package database

import (
	"strconv"

	"go-redis/datastruct/set"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

func (d *DB) getHashSet(key string) (*set.HashSet, resp.ErrorReply) {
	entity, exists := d.getEntity(key)
	if !exists {
		return nil, nil
	}
	hashSet, ok := entity.Data.(*set.HashSet)
	if !ok {
		return nil, reply.NewWrongTypeErrReply()
	}
	return hashSet, nil
}

func (d *DB) getOrCreateHashSet(key string) (hashSet *set.HashSet, create bool, errorReply resp.ErrorReply) {
	hashSet, errorReply = d.getHashSet(key)
	if errorReply != nil {
		return nil, false, errorReply
	}
	create = false
	if hashSet == nil {
		hashSet = set.NewHashSet()
		d.putEntity(key, db.NewDataEntity(hashSet))
		create = true
	}
	return hashSet, create, nil
}

// execSAdd 命令将一个或多个成员元素加入到集合中，已经存在于集合的成员元素将被忽略。
//
// 假如集合 key 不存在，则创建一个只包含添加的元素作成员的集合。
//
// 当集合 key 不是集合类型时，返回一个错误。
//
// # SADD KEY_NAME VALUE1..VALUEN
//
// 返回: 被添加到集合中的新元素的数量，不包括被忽略的元素。
func execSAdd(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	members := args[1:]

	hashSet, _, errReply := d.getOrCreateHashSet(key)
	if errReply != nil {
		return errReply
	}
	counter := 0
	for _, member := range members {
		counter += hashSet.Add(utils.Bytes2String(member))
	}
	d.append(utils.ToCmdLine2(enum.SADD.String(), args...))
	return reply.NewIntReply(int64(counter))
}

// getNums 从参数中获取数字类型, 并返回列表
//
// 如果参数全是数字, 返回true, 否则返回false
func getNums(members [][]byte) (isAllNums bool, nums []int64) {
	nums = make([]int64, len(members))
	for i, member := range members {
		parseInt, err := strconv.ParseInt(utils.Bytes2String(member), 10, 64)
		if err != nil {
			return false, nil
		}
		nums[i] = parseInt
	}
	return true, nums
}

// execSIsMember 命令判断成员元素是否是集合的成员。
//
// # SISMEMBER KEY VALUE
//
// 如果成员元素是集合的成员，返回 1 。 如果成员元素不是集合的成员，或 key 不存在，返回 0 。
func execSIsMember(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	member := utils.Bytes2String(args[1])

	hashSet, errReply := d.getHashSet(key)
	if errReply != nil {
		return errReply
	}
	if hashSet == nil {
		return reply.NewIntReply(0)
	}

	return utils.If(hashSet.Contains(member), reply.NewIntReply(1), reply.NewIntReply(0))
}

// execSRem 命令用于移除集合中的一个或多个成员元素，不存在的成员元素会被忽略。
//
// 当 key 不是集合类型，返回一个错误。
//
// # SREM KEY MEMBER1..MEMBERN
//
// 返回被成功移除的元素的数量，不包括被忽略的元素。
func execSRem(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])
	members := args[1:]

	hashSet, errReply := d.getHashSet(key)
	if errReply != nil {
		return errReply
	}
	if hashSet == nil {
		return reply.NewIntReply(0)
	}
	counter := 0
	for _, member := range members {
		counter += hashSet.Remove(utils.Bytes2String(member))
	}
	if hashSet.Len() == 0 {
		d.Remove(key)
	}
	if counter > 0 {
		d.append(utils.ToCmdLine2(enum.SREM.String(), args...))
	}
	return reply.NewIntReply(int64(counter))
}

// execSPop 命令用于移除集合中的指定 key 的一个或多个随机元素，移除后会返回移除的元素。
//
// # SPOP key [count]
//
// 返回被移除的随机元素。 当集合不存在或是空集时，返回 nil 。
func execSPop(d *DB, args db.Params) resp.Reply {
	if len(args) != 1 && len(args) != 2 {
		return reply.NewArgNumErrReplyByCmd(enum.SPOP)
	}
	key := utils.Bytes2String(args[0])

	hashSet, errReply := d.getHashSet(key)
	if errReply != nil {
		return errReply
	}
	if hashSet == nil {
		return reply.NewNullBulkReply()
	}

	count := 1
	if len(args) == 2 {
		var err error
		count, err = strconv.Atoi(utils.Bytes2String(args[1]))
		if err != nil || count <= 0 {
			return reply.NewErrReply("value is out of range, must be positive")
		}
	}
	if count > hashSet.Len() {
		count = hashSet.Len()
	}

	members := hashSet.RandomDistinctMembers(count)
	result := make([][]byte, len(members))
	for i, v := range members {
		hashSet.Remove(v)
		result[i] = utils.String2Bytes(v)
	}

	if count > 0 {
		d.append(utils.ToCmdLine2(enum.SPOP.String(), args...))
	}
	return reply.NewMultiBulkReply(result)
}

// execSCard 命令返回集合中元素的数量。
//
// # SCARD KEY_NAME
//
// 返回集合的数量。当集合 key 不存在时，返回 0 。
func execSCard(d *DB, args db.Params) resp.Reply {
	key := utils.Bytes2String(args[0])

	hashSet, errReply := d.getHashSet(key)
	if errReply != nil {
		return errReply
	}
	if hashSet == nil {
		return reply.NewIntReply(0)
	}
	return reply.NewIntReply(int64(hashSet.Len()))
}

func set2Reply(hashSet *set.HashSet) resp.Reply {
	arr := make([][]byte, 0, hashSet.Len())
	hashSet.ForEach(func(member string) bool {
		arr = append(arr, utils.String2Bytes(member))
		return true
	})
	return reply.NewMultiBulkReply(arr)
}

// execSInter  命令返回给定所有给定集合的交集。
// 不存在的集合 key 被视为空集。 当给定集合当中有一个空集时，结果也为空集(根据集合运算定律)。
//
// # SINTER KEY KEY1..KEYN
//
// 返回交集成员的列表
func execSInter(d *DB, args db.Params) resp.Reply {
	sets, errReply := getSets(d, args)
	if errReply != nil {
		return reply.NewEmptyMultiBulkReply()
	}
	result := set.Intersect(sets...)
	return set2Reply(result)
}

// execSInterStore 命令将给定集合之间的交集存储在指定的集合中。如果指定的集合已经存在，则将其覆盖。
//
// # SINTERSTORE DESTINATION_KEY KEY KEY1..KEYN
//
// 返回存储交集的集合的元素数量
func execSInterStore(d *DB, args db.Params) resp.Reply {
	dest := utils.Bytes2String(args[0])
	sets, errReply := getSets(d, args)
	if errReply != nil {
		return reply.NewIntReply(0)
	}
	result := set.Intersect(sets...)

	d.Remove(dest) // 刷新ttl
	d.putEntity(dest, db.NewDataEntity(result))
	d.append(utils.ToCmdLine2(enum.SINTERSTORE.String(), args...))
	return reply.NewIntReply(int64(result.Len()))
}

// execSUnion 命令返回给定集合的并集。不存在的集合 key 被视为空集。
//
// # SUNION KEY KEY1..KEYN
//
// 返回 并集成员的列表。
func execSUnion(d *DB, args db.Params) resp.Reply {
	sets, errReply := getSets(d, args)
	if errReply != nil {
		return errReply
	}
	result := set.Union(sets...)
	return set2Reply(result)
}

// execSUnionStore 命令将给定集合的并集存储在指定的集合 destination 中。如果 destination 已经存在，则将其覆盖。
//
// # SUNIONSTORE destination key [key ...]
//
// 返回 结果集中的元素数量
func execSUnionStore(d *DB, args db.Params) resp.Reply {
	dest := utils.Bytes2String(args[0])
	sets, errReply := getSets(d, args[1:])
	if errReply != nil {
		return errReply
	}
	result := set.Union(sets...)
	d.Remove(dest)
	if result.Len() == 0 {
		return reply.NewIntReply(0)
	}

	d.Remove(dest) // 刷新ttl
	d.putEntity(dest, db.NewDataEntity(result))
	d.append(utils.ToCmdLine2(enum.SUNIONSTORE.String(), args...))
	return reply.NewIntReply(int64(result.Len()))
}

// execSDiff 命令返回第一个集合与其他集合之间的差异，也可以认为说第一个集合中独有的元素。不存在的集合 key 将视为空集。
//
// 差集的结果来自前面的 FIRST_KEY ,而不是后面的 OTHER_KEY1，也不是整个 FIRST_KEY OTHER_KEY1..OTHER_KEYN 的差集。
//
// # SDIFF FIRST_KEY OTHER_KEY1..OTHER_KEYN
//
// 返回包含差集成员的列表
func execSDiff(d *DB, args db.Params) resp.Reply {
	sets, errorReply := getSets(d, args)
	if errorReply != nil {
		return errorReply
	}
	result := set.Diff(sets...)
	return set2Reply(result)
}

// execSDiffStore 命令将给定集合之间的差集存储在指定的集合中。如果指定的集合 key 已存在，则会被覆盖。/
//
// # SDIFFSTORE DESTINATION_KEY KEY1..KEYN
//
// 返回结果集中的元素数量
func execSDiffStore(d *DB, args db.Params) resp.Reply {
	dest := utils.Bytes2String(args[0])
	sets, errReply := getSets(d, args[1:])
	if errReply != nil {
		return errReply
	}
	result := set.Diff(sets...)
	d.Remove(dest) // clean ttl
	if result.Len() == 0 {
		return reply.NewIntReply(0)
	}
	d.putEntity(dest, db.NewDataEntity(result))
	d.append(utils.ToCmdLine2(enum.SDIFFSTORE.String(), args...))
	return reply.NewIntReply(int64(result.Len()))
}

// execSRandMember 命令用于返回集合中的一个随机元素。
//
// 1. 如果 count 为正数，且小于集合基数，那么命令返回一个包含 count 个元素的数组，数组中的元素各不相同。如果 count 大于等于集合基数，那么返回整个集合。
//
// 2. 如果 count 为负数，那么命令返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值。
//
// # SRANDMEMBER KEY [count]
//
// 只提供集合 key 参数时，返回一个元素；如果集合为空，返回 nil 。 如果提供了 count 参数，那么返回一个数组；如果集合为空，返回空数组。
func execSRandMember(d *DB, args db.Params) resp.Reply {
	if len(args) != 1 && len(args) != 2 {
		return reply.NewArgNumErrReplyByCmd(enum.SRANDMEMBER)
	}
	key := utils.Bytes2String(args[0])

	hashSet, errReply := d.getHashSet(key)
	if errReply != nil {
		return errReply
	}
	if hashSet == nil {
		return reply.NewNullBulkReply()
	}
	if len(args) == 1 {
		members := hashSet.RandomMembers(1)
		return reply.NewBulkReply(utils.String2Bytes(members[0]))
	}
	count, err := strconv.Atoi(utils.Bytes2String(args[1]))
	if err != nil {
		return reply.NewIntErrReply()
	}
	if count > 0 {
		members := hashSet.RandomDistinctMembers(count)
		result := make([][]byte, len(members))
		for i, v := range members {
			result[i] = []byte(v)
		}
		return reply.NewMultiBulkReply(result)
	} else if count < 0 {
		members := hashSet.RandomMembers(-count)
		result := make([][]byte, len(members))
		for i, v := range members {
			result[i] = utils.String2Bytes(v)
		}
		return reply.NewMultiBulkReply(result)
	}
	return reply.NewEmptyMultiBulkReply()
}

// execSMembers 命令返回集合中的所有的成员。 不存在的集合 key 被视为空集合。
//
// # SMEMBERS key
//
// 返回集合中的所有成员。
func execSMembers(d *DB, args db.Params) resp.Reply {
	key := string(args[0])

	hashSet, errReply := d.getHashSet(key)
	if errReply != nil {
		return errReply
	}
	if hashSet == nil {
		return reply.NewEmptyMultiBulkReply()
	}

	arr := make([][]byte, hashSet.Len())
	i := 0
	hashSet.ForEach(func(member string) bool {
		arr[i] = utils.String2Bytes(member)
		i++
		return true
	})
	return reply.NewMultiBulkReply(arr)
}

// getSets 获取参数中的所有key对应的set, 返回一个set切片
func getSets(d *DB, args db.Params) ([]*set.HashSet, resp.ErrorReply) {
	sets := make([]*set.HashSet, 0, len(args))
	for _, arg := range args {
		key := utils.Bytes2String(arg)
		hashSet, errReply := d.getHashSet(key)
		if errReply != nil {
			return nil, errReply
		}
		sets = append(sets, hashSet)
	}
	return sets, nil
}

func init() {
	registerCommand(enum.SADD, writeFirstKey, execSAdd)
	registerCommand(enum.SCARD, readFirstKey, execSCard)
	registerCommand(enum.SDIFF, readFirstKey, execSDiff)
	registerCommand(enum.SDIFFSTORE, prepareSetCalculateStore, execSDiffStore)
	registerCommand(enum.SINTER, prepareSetCalculate, execSInter)
	registerCommand(enum.SINTERSTORE, prepareSetCalculateStore, execSInterStore)
	registerCommand(enum.SISMEMBER, readFirstKey, execSIsMember)
	registerCommand(enum.SMEMBERS, readFirstKey, execSMembers)
	registerCommand(enum.SPOP, writeFirstKey, execSPop)
	registerCommand(enum.SRANDMEMBER, readFirstKey, execSRandMember)
	registerCommand(enum.SREM, writeFirstKey, execSRem)
	registerCommand(enum.SUNION, prepareSetCalculate, execSUnion)
	registerCommand(enum.SUNIONSTORE, prepareSetCalculateStore, execSUnionStore)
}
