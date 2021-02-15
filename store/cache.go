package kvstore

import (
	"fmt"
	lru "github.com/hashicorp/golang-lru"
)

type Cache interface {
	Get(key string) (value string, ok bool)
	Add(key string, value string)
	Remove(key string)
}

type SimpleCache struct {
	KvMap map[string]string
}

func (s *SimpleCache) Add(key string, value string) {
	s.KvMap[key] = value
}

func (s *SimpleCache) Get(key string) (value string, ok bool) {
	value, ok = s.KvMap[key]
	return value, ok
}

func (s *SimpleCache) Remove(key string) {
	delete(s.KvMap, key)
}

type LruCache struct {
	Lru *lru.ARCCache
}

func (l *LruCache) Add(key string, value string) {
	l.Lru.Add(key, value)
}

func (l *LruCache) Get(key string) (value string, ok bool) {
	var v interface{}
	v, ok = l.Lru.Get(key)
	value = fmt.Sprintf("%v", v)
	return value, ok
}

func (l *LruCache) Remove(key string) {
	l.Lru.Remove(key)
}

func NewLruCache() (Cache, error) {
	var cache *lru.ARCCache
	cache, err := lru.NewARC(1000)
	return &LruCache{cache}, err
}

func NewSimpleCache() (Cache, error) {
	kvMap := make(map[string]string)
	return &SimpleCache{kvMap}, nil
}
