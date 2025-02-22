package database

import (
	"strconv"

	"go-redis/aof"
	set2 "go-redis/datastruct/set"
	"go-redis/enum"
	"go-redis/interface/db"
	"go-redis/lib/utils"
)

// rollbackKeys 回滚key的函数
//
// 1. 如果key原本不存在，回滚操作就是把key删除
//
// 2. 如果key存在, 回滚操作就是把全部数据恢复
//
// 因为是恢复全部数据，所以效率低下，占用内存多. 尽量为命令特定undo函数
func rollbackKeys(d *DB, keys ...string) (cmds []db.CmdLine) {
	for _, key := range keys {
		entity, exist := d.getEntity(key)
		if !exist { // 如果key原本不存在，回滚操作就是把key删除
			cmds = append(cmds, utils.ToCmdLine(enum.DEL.String(), key))
		} else { // 如果key存在, 回滚操作就是把数据恢复
			cmds = append(cmds, utils.ToCmdLine(enum.DEL.String(), key),
				aof.Entity2Cmd(key, entity).Args,
				toTTLCmd(d, key).Args) // 如果数据有过期时间，那么过期时间也要一起恢复
		}
	}
	return
}

func rollbackFirstKey(d *DB, args db.Params) (cmds []db.CmdLine) {
	return rollbackKeys(d, utils.Bytes2String(args[0]))
}

func rollbackHashFields(d *DB, key string, fields ...string) (cmds []db.CmdLine) {
	dict, errReply := d.getDict(key)
	if errReply != nil {
		return nil
	}
	// 1. dict原本不存在, 重做要删除
	if dict == nil {
		cmds = append(cmds,
			utils.ToCmdLine(enum.DEL.String(), key),
		)
		return
	}
	// 2. dict存在，重做要创建
	for _, field := range fields {
		entity, ok := dict.Get(field)
		if !ok {
			cmds = append(cmds,
				utils.ToCmdLine(enum.HDEL.String(), key, field),
			)
		} else {
			cmds = append(
				cmds,
				utils.ToCmdLine(
					enum.HSET.String(),
					key,
					field,
					utils.Bytes2String(parseAny(entity)), // 如果hashtable可以修改value的底层数据, 那么在这里不要共享内存
				),
			)
		}
	}

	return
}

func rollbackSetMembers(d *DB, key string, members ...string) (cmds []db.CmdLine) {
	set, errReply := d.getSet(key)
	if errReply != nil {
		return nil
	}
	if set == nil {
		cmds = append(cmds,
			utils.ToCmdLine(enum.DEL.String(), key),
		)
		return cmds
	}

	removed := make([][]byte, 0)
	added := make([][]byte, 0)

	if isAllNums(members...) {
		switch set.(type) {
		case *set2.IntSet: // 全是数字 且 是intset
			for _, member := range members {
				num, _ := strconv.ParseInt(member, 10, 64)
				ok := set.Contains(num)

				if !ok {
					removed = append(removed, utils.String2Bytes(member))
					/*cmds = append(cmds,
						utils.ToCmdLine(enum.SREM.String(), key, member),
					)*/
				} else {
					added = append(added, utils.String2Bytes(member))
					/*cmds = append(cmds,
						utils.ToCmdLine(enum.SADD.String(), key, member),
					)*/
				}
			}
		case *set2.HashSet: // 全是数字 且 是hashset
			for _, member := range members {
				ok := set.Contains(member)
				if !ok {
					removed = append(removed, utils.String2Bytes(member))
					/*cmds = append(cmds,
						utils.ToCmdLine(enum.SREM.String(), key, member),
					)*/
				} else {
					added = append(added, utils.String2Bytes(member))
					/*cmds = append(cmds,
						utils.ToCmdLine(enum.SADD.String(), key, member),
					)*/
				}
			}
		}
	} else {
		switch set.(type) {
		case *set2.IntSet: // 不全是数字 且 是intset, sadd会进行编码升级
			cmds = append(cmds, aof.Entity2Cmd(key, db.NewDataEntity(set)).Args)
		case *set2.HashSet: // 不全是数字 且 是hashset
			for _, member := range members {
				ok := set.Contains(member)
				if !ok {
					removed = append(removed, utils.String2Bytes(member))
					/*cmds = append(cmds,
						utils.ToCmdLine(enum.SREM.String(), key, member),
					)*/
				} else {
					added = append(added, utils.String2Bytes(member))
					/*cmds = append(cmds,
						utils.ToCmdLine(enum.SADD.String(), key, member),
					)*/
				}
			}
		}
	}

	if len(removed) > 0 {
		cmds = append(cmds, utils.ToCmdLine2(enum.SREM.String(), removed...))
	}

	if len(added) > 0 {
		cmds = append(cmds, utils.ToCmdLine2(enum.SADD.String(), added...))
	}

	return cmds
}

func isAllNums(members ...string) bool {
	for _, member := range members {
		_, err := strconv.ParseInt(member, 10, 64)
		if err != nil {
			return false
		}
	}
	return true
}

func rollbackZSetMembers(d *DB, key string, members ...string) (cmds []db.CmdLine) {
	zset, errReply := d.getSortedSet(key)
	if errReply != nil {
		return nil
	}
	if zset == nil {
		cmds = append(cmds,
			utils.ToCmdLine(enum.DEL.String(), key),
		)
		return
	}
	for _, member := range members {
		element, ok := zset.Get(member)
		if !ok {
			cmds = append(cmds,
				utils.ToCmdLine(enum.ZREM.String(), key, member),
			)
		} else {
			score := strconv.FormatFloat(element.Score, 'f', -1, 64)
			cmds = append(cmds,
				utils.ToCmdLine(enum.ZADD.String(), key, score, member),
			)
		}
	}

	return
}
