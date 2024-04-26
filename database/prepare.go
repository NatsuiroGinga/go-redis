package database

import (
	"go-redis/interface/db"
	"go-redis/lib/utils"
)

func writeAllKeys(args db.Params) (writeKeys, readKeys []string) {
	writeKeys = utils.CopySlices(args)
	return
}

func writeFirstKey(args db.Params) (writeKeys, readKeys []string) {
	writeKeys = utils.CopySlices(args[:1])
	return
}

func readAllKeys(args db.Params) (writeKeys, readKeys []string) {
	readKeys = utils.CopySlices(args)
	return
}

func readFirstKey(args db.Params) (writeKeys, readKeys []string) {
	readKeys = utils.CopySlices(args[:1])
	return
}

func noPrepare(_ db.Params) (writeKeys, readKeys []string) {
	return nil, nil
}

func prepareRename(args db.Params) (writeKeys, readKeys []string) {
	// 1. src上读锁
	writeKeys = utils.CopySlices(args[:1])
	// 2. dst上写锁
	readKeys = utils.CopySlices(args[1:])
	return
}

// prepareSetCalculateStore 是SDIFFSTORE, SINTERSTORE, SUNIONSTORE的准备函数
func prepareSetCalculateStore(args db.Params) (writeKeys, readKeys []string) {
	dest := utils.Bytes2String(args[0])
	keys := make([]string, len(args)-1)
	for i, arg := range args[1:] {
		keys[i] = utils.Bytes2String(arg)
	}
	return []string{dest}, keys
}

// prepareSetCalculate 是SDIFF, SINTER, SUNION的准备函数
func prepareSetCalculate(args db.Params) (writeKeys []string, readKeys []string) {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	return nil, keys
}

func prepareMSet(args [][]byte) (writeKeys []string, readKeys []string) {
	size := len(args) / 2
	keys := make([]string, size)
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i])
	}
	return keys, nil
}
