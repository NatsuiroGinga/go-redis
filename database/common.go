package database

import (
	"errors"
	"strconv"

	"go-redis/interface/db"
	"go-redis/interface/resp"
	"go-redis/lib/asserts"
	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

// computeInterval 根据start和stop计算合法区间, 左闭右闭
//
// # 注意: 会把stop + 1
func computeInterval[T utils.Signed](size T, start, stop *T) error {
	asserts.Assert(size > 0)
	// 1. 计算start
	// 1.1 start < -size, 从0开始
	if *start < -1*size {
		*start = 0
	} else if *start < 0 { // 1.2 -size <= start < 0, 说明是倒数, 从size + start开始
		*start = size + *start
	} else if *start >= size { // 1.3 start >= size, 超出范围
		return errors.New("start out of range")
	}
	// 2. 计算stop
	// 2.1 stop < -size, 在0结束
	if *stop < -1*size {
		*stop = 0
	} else if *stop < 0 { // 2.2 -size <= stop < 0, 说明是倒数, 且是闭区间, 在size + *stop + 1结束
		*stop = size + *stop + 1
	} else if *stop < size { // 2.3 因为是闭区间, 所以加1
		*stop++
	} else { // 2.4 stop >= size, 在size结束
		*stop = size
	}
	// 3. 区间不合法, 设定为[start, start]
	if *stop < *start {
		*stop = *start
	}
	return nil
}

// getInterval 根据命令的参数获取区间的两端
func getInterval(args db.Params) (start, stop int64, errorReply resp.ErrorReply) {
	var err error
	start, err = strconv.ParseInt(utils.Bytes2String(args[1]), 10, 64)
	if err != nil {
		return -1, -1, reply.NewIntErrReply()
	}
	stop, err = strconv.ParseInt(utils.Bytes2String(args[2]), 10, 64)
	if err != nil {
		return -1, -1, reply.NewIntErrReply()
	}
	return
}
