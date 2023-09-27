package plru

import (
	"math"
	"sync"

	"github.com/coyove/sdss/future"
)

const Lookahead = 2

type lruValue[V any] struct {
	Time  int64
	Value V
}

type Cache[K comparable, V any] struct {
	mu       sync.RWMutex
	onEvict  func(K, V)
	storeCap int
	ptr      int
	store    Map[K, lruValue[V]]
}

func New[K comparable, V any](cap int, hash func(K) uint64, onEvict func(K, V)) *Cache[K, V] {
	if cap < Lookahead {
		cap = Lookahead
	}
	if onEvict == nil {
		onEvict = func(K, V) {}
	}
	c := &Cache[K, V]{
		onEvict:  onEvict,
		storeCap: cap,
		store:    *NewMap[K, lruValue[V]](cap/3*2, hash),
	}
	c.store.Fixed = true
	return c
}

func (m *Cache[K, V]) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.store.Len()
}

func (m *Cache[K, V]) Cap() int {
	return m.storeCap
}

func (m *Cache[K, V]) Update(key K, f func(V) V) {
	m.mu.Lock()
	old, ok := m.store.Get(key)
	if ok {
		old.Value = f(old.Value)
		old.Time = future.UnixNano()
		m.store.Set(key, old)
	}
	m.mu.Unlock()
	if ok {
		return
	}
	var null V
	m.Add(key, f(null))
}

func (m *Cache[K, V]) Add(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.store.Set(key, lruValue[V]{
		Time:  future.UnixNano(),
		Value: value,
	})

	if m.store.Len() <= m.storeCap {
		return
	}

	var k0 K
	var v0 = lruValue[V]{Time: math.MaxInt64}
	var e *hashItem[K, lruValue[V]]
	for i := 0; i < Lookahead; i++ {
		m.ptr, e = m.store.nextItem(m.ptr)
		if e.val.Time < v0.Time {
			k0, v0 = e.key, e.val
		}
		m.ptr = (m.ptr + 1) % len(m.store.items)
	}
	m.store.Delete(k0)
	m.onEvict(k0, v0.Value)
}

func (m *Cache[K, V]) Get(k K) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.store.Get(k)
	if ok {
		v.Time = future.UnixNano()
		m.store.Set(k, v)
	}
	return v.Value, ok
}

func (m *Cache[K, V]) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store.Clear()
}

func (m *Cache[K, V]) Delete(key K) V {
	m.mu.Lock()
	defer m.mu.Unlock()
	old, ok := m.store.Delete(key)
	if ok {
		m.onEvict(key, old.Value)
	}
	return old.Value
}

func (m *Cache[K, V]) Range(f func(K, V) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.store.Foreach(func(k K, v *lruValue[V]) bool {
		return f(k, v.Value)
	})
}
