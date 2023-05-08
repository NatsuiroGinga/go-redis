package cluster_database

import "errors"

var (
	TYPE_MISMATCH  = errors.New("type mismatch")
	PEER_NOT_FOUND = errors.New("peer not found")
)
