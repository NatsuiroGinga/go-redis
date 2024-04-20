package dict

// NormalDict 普通的dict, 并发不安全
type NormalDict struct {
	m map[string]any
}

func (nd *NormalDict) Get(key string) (value any, isExist bool) {
	value, isExist = nd.m[key]
	return
}

func (nd *NormalDict) GetWithLock(_ string) (value any, isExist bool) {
	panic("implement me")
}

func (nd *NormalDict) Set(key string, value any) (n int) {
	_, ok := nd.m[key]
	nd.m[key] = value
	if ok {
		return 0
	}
	return 1
}

func (nd *NormalDict) SetWithLock(_ string, _ any) (n int) {
	panic("implement me")
}

func (nd *NormalDict) Len() (n int) {
	return len(nd.m)
}

func (nd *NormalDict) PutIfAbsent(key string, value any) (n int) {
	_, existed := nd.m[key]
	if existed {
		return 0
	}
	nd.m[key] = value
	return 1
}

func (nd *NormalDict) PutIfAbsentWithLock(_ string, _ any) (n int) {
	panic("implement me")
}

func (nd *NormalDict) PutIfExist(key string, value any) (n int) {
	_, existed := nd.m[key]
	if existed {
		nd.m[key] = value
		return 1
	}
	return 0
}

func (nd *NormalDict) PutIfExistWithLock(_ string, _ any) (n int) {
	panic("implement me")
}

func (nd *NormalDict) Remove(key string) (n int) {
	_, existed := nd.m[key]
	if !existed {
		return 0
	}
	delete(nd.m, key)
	return 1
}

func (nd *NormalDict) RemoveWithLock(_ string) (n int) {
	panic("implement me")
}

func (nd *NormalDict) ForEach(consumer consumer) {
	for k, v := range nd.m {
		if !consumer(k, v) {
			break
		}
	}
}

func (nd *NormalDict) Keys() (keys []string) {
	keys = make([]string, 0, len(nd.m))
	for k := range nd.m {
		keys = append(keys, k)
	}
	return
}

func (nd *NormalDict) RandomKeys(n int) (keys []string) {
	keys = make([]string, n)
	for i := 0; i < n; i++ {
		for k := range nd.m {
			keys[i] = k
			break
		}
	}
	return
}

func (nd *NormalDict) RandomDistinctKeys(n int) (keys []string) {
	size := n
	if size > len(nd.m) {
		size = len(nd.m)
	}
	result := make([]string, size)
	i := 0
	for k := range nd.m {
		if i == size {
			break
		}
		result[i] = k
		i++
	}
	return result
}

func (nd *NormalDict) Clear() {
	*nd = *NewNormalDict()
}

func (nd *NormalDict) RWLocks(_, _ []string) {
	panic("implement me")
}

func (nd *NormalDict) RWUnLocks(_, _ []string) {
	panic("implement me")
}

func NewNormalDict() *NormalDict {
	return &NormalDict{make(map[string]any)}
}
