package kvstore

type Cache interface {
	Get(key string) (value interface{}, ok bool)
	Add(key string, value interface{})
	Remove(key string)
}

type SimpleCache struct {
	KvMap map[string]interface{}
}

func (s *SimpleCache) Add(key string, value interface{}) {
	s.KvMap[key] = value
}

func (s *SimpleCache) Get(key string) (value interface{}, ok bool) {
	value, ok = s.KvMap[key]
	return value, ok
}

func (s *SimpleCache) Remove(key string) {
	delete(s.KvMap, key)
}

func NewSimpleCache() (Cache, error) {
	kvMap := make(map[string]interface{})
	return &SimpleCache{kvMap}, nil
}
