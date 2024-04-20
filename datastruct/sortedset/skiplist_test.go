package sortedset

import (
	"testing"

	"go-redis/lib/logger"
)

func TestRandomLevel(t *testing.T) {
	for i := 0; i < 10; i++ {
		lv := randomLevel()
		logger.Info("lv:", lv)
	}
}

func TestInsert(t *testing.T) {
	l := newSkiplist()
	node := l.insert("jack", 100)
	logger.Info("node:", *node)
}

func TestGetRank(t *testing.T) {
	l := newSkiplist()
	l.insert("jack", 100)
	l.insert("lily", 99)
	l.insert("lily", 0)
	rank := l.getRank("jack", 100)
	logger.Info("rank:", rank)
}

func TestDelete(t *testing.T) {
	l := newSkiplist()
	l.insert("jack", 100)
	l.insert("lily", 99)
	l.insert("lily", 0)
	if l.delete("jack", 100) {
		logger.Info("delete jack ok")
		rank := l.getRank("jack", 100)
		logger.Info("rank:", rank)
	} else {
		logger.Info("fail")
	}
}

func TestGetElementByRank(t *testing.T) {
	l := newSkiplist()
	l.insert("jack", 100)
	l.insert("lily", 99)
	l.insert("oo", 1000)
	node := l.getElementByRank(3)
	logger.Info("node:", *node)
}

func TestUpdateScore(t *testing.T) {
	l := newSkiplist()
	l.insert("lily", 99)
	l.insert("lily", 0)
	node := l.updateScore("jack", 100, 200)
	logger.Info("node:", node)
}
