package aof

import (
	"strconv"
	"time"

	"go-redis/datastruct/dict"
	"go-redis/datastruct/list"
	"go-redis/datastruct/set"
	"go-redis/datastruct/sortedset"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/lib/logger"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// string2Cmd 把string序列化成resp格式的命令
func string2Cmd(key string, val []byte) *reply.MultiBulkReply {
	cmd := utils.ToCmdLine2(enum.SET.String(), utils.String2Bytes(key), val)
	return reply.NewMultiBulkReply(cmd)
}

// Entity2Cmd 把数据实体序列化为resp格式的命令
func Entity2Cmd(key string, entity *db.DataEntity) *reply.MultiBulkReply {
	if entity == nil {
		return nil
	}
	var cmd *reply.MultiBulkReply
	switch val := entity.Data.(type) {
	case []byte:
		cmd = string2Cmd(key, val)
	case list.List:
		cmd = list2Cmd(key, val)
	case set.Set:
		cmd = set2Cmd(key, val)
	case dict.Dict:
		cmd = hash2Cmd(key, val)
	case *sortedset.SortedSet:
		cmd = zset2Cmd(key, val)
	default:
		logger.Error("unknown data type")
	}
	return cmd
}

// list2Cmd 把list序列化成resp格式的命令
func list2Cmd(key string, l list.List) *reply.MultiBulkReply {
	cmd := make([][]byte, l.Len()+2)
	cmd[0] = enum.RPUSH.Bytes()
	cmd[1] = utils.String2Bytes(key)
	l.ForEach(func(i int, val any) bool {
		cmd[2+i] = val.([]byte)
		return true
	})
	return reply.NewMultiBulkReply(cmd)
}

// set2Cmd 把set序列化成resp格式的命令
func set2Cmd(key string, st set.Set) *reply.MultiBulkReply {
	cmd := make([][]byte, 0, st.Len()+2)
	cmd = append(cmd, enum.SADD.Bytes(), utils.String2Bytes(key))
	st.ForEach(func(member any) bool {
		cmd = append(cmd, set.ToBytes(member))
		return true
	})
	return reply.NewMultiBulkReply(cmd)
}

func hash2Cmd(key string, hashTable dict.Dict) *reply.MultiBulkReply {
	cmd := make([][]byte, 0, hashTable.Len()+2)
	cmd = append(cmd, enum.HMSET.Bytes(), utils.String2Bytes(key))
	hashTable.ForEach(func(key string, value any) bool {
		cmd = append(cmd, utils.String2Bytes(key), value.([]byte))
		return true
	})
	return reply.NewMultiBulkReply(cmd)
}

func zset2Cmd(key string, zset *sortedset.SortedSet) *reply.MultiBulkReply {
	cmd := make([][]byte, 0, zset.Length()+2)
	cmd = append(cmd, enum.ZADD.Bytes(), utils.String2Bytes(key))
	zset.ForEachByRank(int64(0), zset.Length(), false, func(e *sortedset.Element) bool {
		score := strconv.FormatFloat(e.Score, 'f', -1, 64)
		cmd = append(cmd, utils.String2Bytes(score), utils.String2Bytes(e.Ele))
		return true
	})
	return reply.NewMultiBulkReply(cmd)
}

func NewExpireCmd(key string, expireTime time.Time) *reply.MultiBulkReply {
	ms := strconv.FormatInt(expireTime.UnixMilli(), 10)
	cmd := utils.ToCmdLine(enum.PEXPIREAT.String(), key, ms)
	return reply.NewMultiBulkReply(cmd)
}
