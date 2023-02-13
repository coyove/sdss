package bitmap

import (
	"container/list"
	"sync"
)

type Cache struct {
	maxWeight int64
	curWeight int64

	ll    *list.List
	cache map[string]*list.Element

	sync.Mutex
}

type entry struct {
	key    string
	value  *Range
	weight int64
}

func NewLRUCache(maxWeight int64) *Cache {
	return &Cache{
		maxWeight: maxWeight,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
	}
}

func (c *Cache) Add(key string, value *Range) {
	if c.maxWeight <= 0 {
		return
	}
	weight := value.RoughSizeBytes()
	if weight > c.maxWeight {
		weight = c.maxWeight
	}

	c.Lock()
	defer c.Unlock()

	if ee, ok := c.cache[key]; ok {
		e := ee.Value.(*entry)
		c.ll.MoveToFront(ee)
		diff := weight - e.weight
		e.weight = weight
		e.value = value
		c.curWeight += diff
	} else {
		c.curWeight += weight
		ele := c.ll.PushFront(&entry{key, value, weight})
		c.cache[key] = ele
	}

	for c.maxWeight > 0 && c.curWeight > c.maxWeight {
		last := c.ll.Back()
		kv := last.Value.(*entry)
		c.ll.Remove(last)
		c.curWeight -= last.Value.(*entry).weight
		delete(c.cache, kv.key)
	}
}

func (c *Cache) Get(key string) *Range {
	c.Lock()
	defer c.Unlock()

	if ele, hit := c.cache[key]; hit {
		e := ele.Value.(*entry)
		c.ll.MoveToFront(ele)
		return e.value
	}
	return nil
}

func (c *Cache) Len() (len int) {
	c.Lock()
	len = c.ll.Len()
	c.Unlock()
	return
}
