package database

import (
	"math"
	"strconv"
	"testing"

	"go-redis/datastruct/set"
	"go-redis/enum"
	"go-redis/interface/resp"
	"go-redis/lib/utils"
)

func TestExecSAdd(t *testing.T) {
	args := utils.ToCmdLine(
		enum.SADD.String(),
		"nums",
		"1",
		"2",
		"3",
		strconv.FormatInt(math.MaxInt16+1, 10),
		strconv.FormatInt(math.MaxInt32+1, 10),
		"jack",
		"lily",
	)
	reply := testDB.exec(args)
	if errReply, ok := reply.(resp.ErrorReply); ok {
		t.Error(errReply.Error())
	} else {
		t.Log(string(reply.Bytes()))
		TestSMembers(t)
	}
}

func TestSAddOutOfLimit(t *testing.T) {
	args := utils.ToCmdLine(enum.SADD.String(), "nums")
	for i := 0; i < 512; i++ {
		args = append(args, []byte(strconv.Itoa(i)))
	}
	args = append(args, []byte("jack"), []byte("bob"))
	reply := testDB.exec(args)
	if errReply, ok := reply.(resp.ErrorReply); ok {
		t.Error(errReply.Error())
	} else {
		t.Log(string(reply.Bytes()))
		st, _ := testDB.getSet("nums")
		t.Log("counter:", string(reply.Bytes()))
		if _, succ := st.(*set.IntSet); succ {
			t.Log("set type: intset", st)
		} else {
			t.Log("set type: hashset")
			reply = testDB.exec(utils.ToCmdLine(enum.SMEMBERS.String(), "nums"))
			t.Log(string(reply.Bytes()))
		}
	}
}

func TestSMembers(t *testing.T) {
	key := "nums"
	args := utils.ToCmdLine(
		enum.SMEMBERS.String(),
		key,
	)
	reply := testDB.exec(args)
	if errReply, ok := reply.(resp.ErrorReply); ok {
		t.Error(errReply.Error())
	} else {
		st, _ := testDB.getSet(key)
		t.Log("counter:", string(reply.Bytes()))
		if _, succ := st.(*set.IntSet); succ {
			t.Log("set type: intset", st)
		} else {
			t.Log("set type: hashset", st)
		}
	}
}

func TestSRem(t *testing.T) {
	TestExecSAdd(t)
	args := utils.ToCmdLine(enum.SREM.String(), "nums", "jack", "1")
	reply := testDB.exec(args)
	if errReply, ok := reply.(resp.ErrorReply); ok {
		t.Error(errReply.Error())
	} else {
		t.Log(string(reply.Bytes()))
		TestSMembers(t)
	}
}

func TestSIsMember(t *testing.T) {
	TestExecSAdd(t)
	args := utils.ToCmdLine(enum.SISMEMBER.String(), "nums", "32768")
	reply := testDB.exec(args)
	if errReply, ok := reply.(resp.ErrorReply); ok {
		t.Error(errReply.Error())
	} else {
		t.Log(string(reply.Bytes()))
	}
}

func TestSPop(t *testing.T) {
	TestExecSAdd(t)
	args := utils.ToCmdLine(enum.SPOP.String(), "nums", "2")
	reply := testDB.exec(args)
	if errReply, ok := reply.(resp.ErrorReply); ok {
		t.Error(errReply.Error())
	} else {
		t.Log(string(reply.Bytes()))
		TestSMembers(t)
	}
}

func TestSInter(t *testing.T) {
	intSet := set.NewIntSet()
	intSet.Add(int64(1))
	intSet.Add(int64(2))
	intSet.Add(int64(3))

	hashSet := set.NewIntSet()
	hashSet.Add(1)
	hashSet.Add(3)
	intersect := set.Intersect(intSet, hashSet)
	t.Log(intersect.Len())
	slice := intersect.ToSlice()
	t.Log(slice)
}

func TestSUnion(t *testing.T) {
	intSet := set.NewIntSet()
	for i := 0; i < 5; i++ {
		intSet.Add(i)
	}

	hashSet := set.NewHashSet()
	hashSet.Add(strconv.Itoa(1))
	hashSet.Add(strconv.Itoa(2))
	hashSet.Add("jack")
	hashSet.Add("bob")
	intersect := set.Intersect(intSet, hashSet)
	t.Log(intersect.Len())
	slice := intersect.ToSlice()
	t.Log(slice)
}

func TestSDiff(t *testing.T) {
	intSet := set.NewHashSet()
	for i := 0; i < 5; i++ {
		intSet.Add(strconv.Itoa(i)) // 0 1 2 3 4
	}

	hashSet := set.NewHashSet() // 1 2 jack bob
	// hashSet.Add(1)
	// hashSet.Add(2)
	hashSet.Add("jack")
	hashSet.Add("bob")

	diff := set.Diff(intSet, hashSet)
	t.Log(diff.ToSlice())
}

func TestRandomMembers(t *testing.T) {
	TestExecSAdd(t)
	reply := testDB.exec(utils.ToCmdLine(enum.SRANDMEMBER.String(), "nums", "2"))
	if errReply, ok := reply.(resp.ErrorReply); ok {
		t.Error(errReply.Error())
	} else {
		t.Log(string(reply.Bytes()))
	}
}

func TestSDiffStore(t *testing.T) {
	TestSDiff(t)
}
