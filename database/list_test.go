package database

import (
	"testing"

	"go-redis/lib/utils"
	"go-redis/resp/reply"
)

var testDB *DB

func init() {
	testDB = newDB(0)
}

func TestPush(t *testing.T) {
	testDB.Flush()
	size := 100

	// rpush single
	key := utils.RandString(10)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils.RandString(10)
		values[i] = []byte(value)
		result := testDB.exec(utils.ToCmdLine("rpush", key, value))
		if intResult, _ := result.(*reply.IntReply); intResult.Code() != int64(i+1) {
			t.Errorf("expected %d, actually %d", i+1, intResult.Code())
		}
	}
	actual := testDB.exec(utils.ToCmdLine("lrange", key, "0", "-1"))
	expected := reply.NewMultiBulkReply(values)
	if !utils.BytesEquals(actual.Bytes(), expected.Bytes()) {
		t.Error("push error")
	}
	testDB.Remove(key)

	// rpush multi
	key = utils.RandString(10)
	args := make([][]byte, size+1)
	args[0] = []byte(key)
	values = make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils.RandString(10)
		values[i] = []byte(value)
		args[i+1] = []byte(value)
	}
	result := testDB.exec(utils.ToCmdLine2("rpush", args...))
	if intResult, _ := result.(*reply.IntReply); intResult.Code() != int64(size) {
		t.Errorf("expected %d, actually %d", size, intResult.Code())
	}
	actual = testDB.exec(utils.ToCmdLine("lrange", key, "0", "-1"))
	expected = reply.NewMultiBulkReply(values)
	if !utils.BytesEquals(actual.Bytes(), expected.Bytes()) {
		t.Error("push error")
	}
	testDB.Remove(key)

	// left push single
	key = utils.RandString(10)
	values = make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils.RandString(10)
		values[size-i-1] = []byte(value)
		result = testDB.exec(utils.ToCmdLine("lpush", key, value))
		if intResult, _ := result.(*reply.IntReply); intResult.Code() != int64(i+1) {
			t.Errorf("expected %d, actually %d", i+1, intResult.Code())
		}
	}
	actual = testDB.exec(utils.ToCmdLine("lrange", key, "0", "-1"))
	expected = reply.NewMultiBulkReply(values)
	if !utils.BytesEquals(actual.Bytes(), expected.Bytes()) {
		t.Error("push error")
	}
	testDB.Remove(key)

	// left push multi
	key = utils.RandString(10)
	args = make([][]byte, size+1)
	args[0] = []byte(key)
	expectedValues := make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils.RandString(10)
		args[i+1] = []byte(value)
		expectedValues[size-i-1] = []byte(value)
	}
	// result = execLPush(testDB, values)
	result = testDB.exec(utils.ToCmdLine2("lpush", args...))
	if intResult, _ := result.(*reply.IntReply); intResult.Code() != int64(size) {
		t.Errorf("expected %d, actually %d", size, intResult.Code())
	}
	actual = testDB.exec(utils.ToCmdLine("lrange", key, "0", "-1"))
	expected = reply.NewMultiBulkReply(expectedValues)
	if !utils.BytesEquals(actual.Bytes(), expected.Bytes()) {
		t.Error("push error")
	}
	testDB.Remove(key)
}

func TestLRange(t *testing.T) {
	// prepare list
	testDB.Flush()
	size := 100
	key := utils.RandString(10)
	values := make([][]byte, size)
	for i := 0; i < size; i++ {
		value := utils.RandString(10)
		testDB.exec(utils.ToCmdLine("rpush", key, value))
		values[i] = []byte(value)
	}

	start := "0"
	end := "9"
	actual := testDB.exec(utils.ToCmdLine("lrange", key, start, end))
	expected := reply.NewMultiBulkReply(values[0:10])
	if !utils.BytesEquals(actual.Bytes(), expected.Bytes()) {
		t.Errorf("range error [%s, %s]", start, end)
	}

	start = "0"
	end = "200"
	actual = testDB.exec(utils.ToCmdLine("lrange", key, start, end))
	expected = reply.NewMultiBulkReply(values)
	if !utils.BytesEquals(actual.Bytes(), expected.Bytes()) {
		t.Errorf("range error [%s, %s]", start, end)
	}

	start = "0"
	end = "-10"
	actual = testDB.exec(utils.ToCmdLine("lrange", key, start, end))
	expected = reply.NewMultiBulkReply(values[0 : size-10+1])
	if !utils.BytesEquals(actual.Bytes(), expected.Bytes()) {
		t.Errorf("range error [%s, %s]", start, end)
	}

	start = "0"
	end = "-200"
	actual = testDB.exec(utils.ToCmdLine("lrange", key, start, end))
	expected = reply.NewMultiBulkReply(values[0:0])
	if !utils.BytesEquals(actual.Bytes(), expected.Bytes()) {
		t.Errorf("range error [%s, %s]", start, end)
	}

	start = "-10"
	end = "-1"
	actual = testDB.exec(utils.ToCmdLine("lrange", key, start, end))
	expected = reply.NewMultiBulkReply(values[90:])
	if !utils.BytesEquals(actual.Bytes(), expected.Bytes()) {
		t.Errorf("range error [%s, %s]", start, end)
	}
}

func TestLPop(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	values := utils.ToCmdLine(key, "a", "b", "c", "d", "e", "f")
	testDB.exec(utils.ToCmdLine2("rpush", values...))
	size := len(values) - 1

	for i := 0; i < size; i++ {
		result := testDB.exec(utils.ToCmdLine("lpop", key))
		expected := reply.NewBulkReply(values[i+1])
		if !utils.BytesEquals(result.Bytes(), expected.Bytes()) {
			t.Errorf("expected %s, actually %s", string(expected.Bytes()), string(result.Bytes()))
		}
	}
	result := testDB.exec(utils.ToCmdLine("rpop", key))
	expected := reply.NewNullBulkReply()
	if !utils.BytesEquals(result.Bytes(), expected.Bytes()) {
		t.Errorf("expected %s, actually %s", string(expected.Bytes()), string(result.Bytes()))
	}
}
