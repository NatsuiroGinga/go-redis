package dict

import (
	"sync"
)

type SyncDict struct {
	m sync.Map
}

func (sd *SyncDict) GetWithLock(key string) (value any, isExist bool) {
	return sd.Get(key)
}

func (sd *SyncDict) SetWithLock(key string, value any) (n int) {
	return sd.Set(key, value)
}

func (sd *SyncDict) PutIfAbsentWithLock(key string, value any) (n int) {
	return sd.PutIfAbsent(key, value)
}

func (sd *SyncDict) PutIfExistWithLock(key string, value any) (n int) {
	return sd.PutIfExist(key, value)
}

func (sd *SyncDict) RemoveWithLock(key string) (n int) {
	return sd.Remove(key)
}

func (sd *SyncDict) RWLocks(writeKeys, readKeys []string) {
	panic("implement me")
}

func (sd *SyncDict) RWUnLocks(writeKeys, readKeys []string) {
	panic("implement me")
}

func (sd *SyncDict) Get(key string) (value any, ok bool) {
	return sd.m.Load(key)
}

func (sd *SyncDict) Set(key string, value any) (n int) {
	_, ok := sd.m.Load(key)
	sd.m.Store(key, value)
	if !ok {
		return 1
	}
	return 0
}

func (sd *SyncDict) Len() (n int) {
	sd.m.Range(func(key, value any) bool {
		n++
		return true
	})
	return n
}

func (sd *SyncDict) PutIfAbsent(key string, value any) (n int) {
	if _, ok := sd.m.LoadOrStore(key, value); !ok { // key 不存在
		sd.m.Store(key, value)
		return 1
	}
	return 0
}

func (sd *SyncDict) PutIfExist(key string, value any) (n int) {
	if _, ok := sd.m.LoadOrStore(key, value); ok { // key 存在
		sd.m.Store(key, value)
		return 1
	}
	return 0
}

func (sd *SyncDict) Remove(key string) (n int) {
	if _, ok := sd.m.Load(key); ok {
		sd.m.Delete(key)
		return 1
	}
	return 0
}

func (sd *SyncDict) ForEach(consumer consumer) {
	sd.m.Range(func(key, value any) bool {
		return consumer(key.(string), value)
	})
}

func (sd *SyncDict) Keys() []string {
	keys := make([]string, 0, sd.Len())

	sd.m.Range(func(key, value any) bool {
		keys = append(keys, key.(string))
		return true
	})

	return keys
}

func (sd *SyncDict) RandomKeys(n int) []string {
	keys := make([]string, 0, n)

	for i := 0; i < n; i++ {
		sd.m.Range(func(key, value any) bool {
			keys = append(keys, key.(string))
			return false
		})
	}

	return keys
}

func (sd *SyncDict) RandomDistinctKeys(n int) []string {
	keys := make([]string, 0, n)

	sd.m.Range(func(key, value any) bool {
		keys = append(keys, key.(string))
		if len(keys) == n {
			return false
		}
		return true
	})

	return keys
}

func (sd *SyncDict) Clear() {
	*sd = *NewSyncDict()
}

func NewSyncDict() *SyncDict {
	return &SyncDict{}
}
