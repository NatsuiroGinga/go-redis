package cluster_database

import (
	"go-redis/interface/db"
	"go-redis/lib/utils"
)

func makeArgs(cmd string, args ...string) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = utils.String2Bytes(cmd)
	for i, arg := range args {
		result[i+1] = utils.String2Bytes(arg)
	}
	return result
}

// groupBy 根据keys选取存储数据的节点, 然后以节点地址为键, 存储在此节点的keys为值, 构建新的索引map
//
// return node -> writeKeys
func (cd *ClusterDatabase) groupBy(keys []string) map[string][]string {
	result := make(map[string][]string)
	for _, key := range keys {
		peer := cd.peerPicker.Pick(key)
		group, ok := result[peer]
		if !ok {
			group = make([]string, 0)
		}
		group = append(group, key)
		result[peer] = group
	}
	return result
}

// addCmdPrefix 给命令添加前缀
func addCmdPrefix(cmdLine db.CmdLine, prefix string) db.CmdLine {
	var cmdLine2 db.CmdLine
	cmdLine2 = append(cmdLine2, cmdLine...)
	cmdLine2[0] = []byte(prefix)
	return cmdLine2
}
