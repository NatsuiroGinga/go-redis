package list

import (
	"strconv"
	"testing"

	"go-redis/config"
	"go-redis/lib/utils"
)

func TestPushBack(t *testing.T) {
	quickList := NewQuickList()
	for i := 0; i < config.Properties.ListMaxShardSize+1; i++ {
		quickList.PushBack(i)
	}
	back := quickList.data.Back()
	val := back.Value.([]any)
	t.Log(val)
}

func TestGet(t *testing.T) {
	quickList := NewQuickList()
	for i := 0; i < config.Properties.ListMaxShardSize+1; i++ {
		quickList.PushBack(i)
	}
	val := quickList.Get(1)
	t.Log(val)
}

func TestRemove(t *testing.T) {
	quickList := NewQuickList()
	for i := 0; i < config.Properties.ListMaxShardSize+1; i++ {
		quickList.PushBack(i)
	}
	t.Log(quickList.Len())
	val := quickList.RemoveLast()
	t.Log(val)
}

func TestQuickListRemoveVal(t *testing.T) {
	list := NewQuickList()
	size := config.Properties.ListMaxShardSize * 2
	for i := 0; i < size; i++ {
		list.PushBack(i)
		list.PushBack(i)
	}
	for index := 0; index < list.Len(); index++ {
		list.RemoveAllByVal(func(a interface{}) bool {
			return utils.Equals(a, index)
		})
		list.ForEach(func(i int, v interface{}) bool {
			intVal, _ := v.(int)
			if intVal == index {
				t.Error("remove test fail: found  " + strconv.Itoa(index) + " at index: " + strconv.Itoa(i))
			}
			return true
		})
	}

	list = NewQuickList()
	for i := 0; i < size; i++ {
		list.PushBack(i)
		list.PushBack(i)
	}
	for i := 0; i < size; i++ {
		list.RemoveByVal(func(a interface{}) bool {
			return utils.Equals(a, i)
		}, 1)
	}
	list.ForEach(func(i int, v interface{}) bool {
		intVal, _ := v.(int)
		if intVal != i {
			t.Error("test fail: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal))
		}
		return true
	})
	for i := 0; i < size; i++ {
		list.RemoveByVal(func(a interface{}) bool {
			return utils.Equals(a, i)
		}, 1)
	}
	if list.Len() != 0 {
		t.Error("test fail: expected 0, actual: " + strconv.Itoa(list.Len()))
	}

	list = NewQuickList()
	for i := 0; i < size; i++ {
		list.PushBack(i)
		list.PushBack(i)
	}
	for i := 0; i < size; i++ {
		list.ReverseRemoveByVal(func(a interface{}) bool {
			return utils.Equals(a, i)
		}, 1)
	}
	list.ForEach(func(i int, v interface{}) bool {
		intVal, _ := v.(int)
		if intVal != i {
			t.Error("test fail: expected " + strconv.Itoa(i) + ", actual: " + strconv.Itoa(intVal))
		}
		return true
	})
	for i := 0; i < size; i++ {
		list.ReverseRemoveByVal(func(a interface{}) bool {
			return utils.Equals(a, i)
		}, 1)
	}
	if list.Len() != 0 {
		t.Error("test fail: expected 0, actual: " + strconv.Itoa(list.Len()))
	}
}
