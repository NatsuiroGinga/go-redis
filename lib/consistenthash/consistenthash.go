package consistenthash

import (
	"hash/crc32"

	"go-redis/lib/utils"
)

// HashFunc is the type of hash function to use to map keys to
type HashFunc func(data []byte) uint32

// NodeMap is the interface that wraps the basic NodeMap methods
type NodeMap struct {
	hashFunc HashFunc          // hash function
	hashes   []uint32          // sorted hashes
	mp       map[uint32]string // hash -> node
}

// NewNodeMap creates a new NodeMap instance
func NewNodeMap(hashFunc HashFunc) *NodeMap {
	return &NodeMap{
		hashFunc: utils.If(hashFunc == nil, crc32.ChecksumIEEE, hashFunc),
		hashes:   make([]uint32, 0),
		mp:       make(map[uint32]string),
	}
}

// IsEmpty check if the NodeMap is empty
//
// return true if the NodeMap is empty, else false
func (nodeMap *NodeMap) IsEmpty() bool {
	return len(nodeMap.hashes) == 0
}

// Add adds some nodes to the NodeMap
//
// return the NodeMap itself
func (nodeMap *NodeMap) Add(nodes ...string) *NodeMap {
	for _, node := range nodes {
		if node == "" {
			continue
		}

		hash := nodeMap.hashFunc(utils.String2Bytes(node))
		nodeMap.hashes = append(nodeMap.hashes, hash)
		nodeMap.mp[hash] = node
	}
	utils.Sort(nodeMap.hashes)

	return nodeMap
}

// Pick picks a node according to the key
func (nodeMap *NodeMap) Pick(key string) string {
	if nodeMap.IsEmpty() {
		return ""
	}

	hash := nodeMap.hashFunc(utils.String2Bytes(key))
	idx := utils.Search(nodeMap.hashes, hash)

	return nodeMap.mp[nodeMap.hashes[idx%len(nodeMap.hashes)]]
}
