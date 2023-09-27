package plru

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type Map[K comparable, V any] struct {
	Fixed bool

	count uint32
	hash  func(K) uint64
	items []hashItem[K, V]
}

// hashItem represents a slot in the map.
type hashItem[K, V any] struct {
	key      K
	val      V
	dist     int32
	occupied bool
}

func NewMap[K comparable, V any](size int, hash func(K) uint64) *Map[K, V] {
	obj := &Map[K, V]{hash: hash}
	obj.items = make([]hashItem[K, V], size*2)
	return obj
}

// Cap returns the capacity of the map, keys more than Cap() will trigger the resizing.
func (m *Map[K, V]) Cap() int {
	return len(m.items)
}

// Len returns the count of keys in the map.
func (m *Map[K, V]) Len() int {
	return int(m.count)
}

// Clear clears all keys in the map, where already allocated memory will be reused.
func (m *Map[K, V]) Clear() {
	for i := range m.items {
		m.items[i] = hashItem[K, V]{}
	}
	m.count = 0
}

// Get retrieves the value by 'k', returns false as the second argument if not found.
func (m *Map[K, V]) Get(k K) (v V, exists bool) {
	if idx := m.findValue(k); idx >= 0 {
		return m.items[idx].val, true
	}
	return v, false
}

func (m *Map[K, V]) findValue(k K) int {
	num := len(m.items)
	if num <= 0 {
		return -1
	}
	idx := int(m.hash(k) % uint64(num))
	idxStart := idx

	for {
		e := &m.items[idx]
		if !e.occupied {
			return -1
		}

		if e.key == k {
			return idx
		}

		idx = (idx + 1) % num
		if idx == idxStart {
			return -1
		}
	}
}

// Contains returns true if the map contains 'k'.
func (m *Map[K, V]) Contains(k K) bool {
	return m.findValue(k) >= 0
}

// Set upserts a key-value pair in the map. Nil key is not allowed.
func (m *Map[K, V]) Set(k K, v V) (prev V) {
	if len(m.items) <= 0 {
		m.items = make([]hashItem[K, V], 8)
	}
	if int(m.count) >= len(m.items)*3/4 {
		m.resizeHash(len(m.items) * 2)
	}
	return m.setHash(hashItem[K, V]{key: k, val: v, occupied: true})
}

// Delete deletes a key from the map, returns deleted value if existed
func (m *Map[K, V]) Delete(k K) (prev V, ok bool) {
	idx := m.findValue(k)
	if idx < 0 {
		return prev, false
	}
	prev = m.items[idx].val

	// Shift the following keys forward
	num := len(m.items)
	startIdx := idx
	current := idx

NEXT:
	next := (current + 1) % num
	if m.items[next].dist > 0 {
		m.items[current] = m.items[next]
		m.items[current].dist--
		current = next
		if current != startIdx {
			goto NEXT
		}
	} else {
		m.items[current] = hashItem[K, V]{}
	}

	m.count--
	return prev, true
}

func (m *Map[K, V]) setHash(incoming hashItem[K, V]) (prev V) {
	num := len(m.items)
	idx := int(m.hash(incoming.key) % uint64(num))

	for idxStart := idx; ; {
		e := &m.items[idx]

		if !e.occupied {
			m.items[idx] = incoming
			m.count++
			return
		}

		if e.key == incoming.key {
			prev = e.val
			e.val, e.dist = incoming.val, incoming.dist
			return prev
		}

		// Swap if the incoming item is further from its best idx.
		if e.dist < incoming.dist {
			incoming, m.items[idx] = m.items[idx], incoming
		}

		incoming.dist++ // one step further away from best idx.
		idx = (idx + 1) % num

		if idx == idxStart {
			panic("fatal: object space not enough")
		}
	}
}

// Foreach iterates all keys in the map, for each of them, 'f(key, &value)' will be
// called. Values are passed by pointers and it is legal to manipulate them directly in 'f'.
func (m *Map[K, V]) Foreach(f func(K, *V) bool) {
	for i := 0; i < len(m.items); i++ {
		ip := &m.items[i]
		if ip.occupied {
			if !f(ip.key, &ip.val) {
				return
			}
		}
	}
}

func (m *Map[K, V]) nextItem(idx int) (int, *hashItem[K, V]) {
	for i := idx; i < len(m.items); i++ {
		if p := &m.items[i]; p.occupied {
			return i, p
		}
	}
	return 0, nil
}

func (m *Map[K, V]) First() (nextk K, nextv V, ok bool) {
	if _, p := m.nextItem(0); p != nil {
		return p.key, p.val, true
	}
	return nextk, nextv, false
}

// Next finds the next key after 'k', returns nil if not found.
func (m *Map[K, V]) Next(k K) (nextk K, nextv V, ok bool) {
	idx := m.findValue(k)
	if idx >= 0 {
		if _, p := m.nextItem(idx + 1); p != nil {
			return p.key, p.val, true
		}
	}
	return nextk, nextv, false
}

func (m *Map[K, V]) Copy() *Map[K, V] {
	m2 := *m
	m2.items = append([]hashItem[K, V]{}, m.items...)
	return &m2
}

func (m *Map[K, V]) Merge(src *Map[K, V]) *Map[K, V] {
	if src.Len() > 0 {
		m.resizeHash((m.Len() + src.Len()) * 2)
		src.Foreach(func(k K, v *V) bool { m.Set(k, *v); return true })
	}
	return m
}

func (m *Map[K, V]) resizeHash(newSize int) {
	if m.Fixed {
		return
	}
	if newSize <= len(m.items) {
		return
	}
	tmp := *m
	tmp.items = make([]hashItem[K, V], newSize)
	for _, e := range m.items {
		if e.occupied {
			e.dist = 0
			tmp.setHash(e)
		}
	}
	m.items = tmp.items
}

func (m *Map[K, V]) density() float64 {
	num := len(m.items)
	if num <= 0 || m.count <= 0 {
		return math.NaN()
	}

	var maxRun int
	for i := 0; i < num; {
		if !m.items[i].occupied {
			i++
			continue
		}
		run := 1
		for i++; i < num; i++ {
			if m.items[i].occupied {
				run++
			} else {
				break
			}
		}
		if run > maxRun {
			maxRun = run
		}
	}
	return float64(maxRun) / (float64(num) / float64(m.count))
}

func (m *Map[K, V]) String() string {
	p := bytes.Buffer{}
	for idx, i := range m.items {
		p.WriteString(strconv.Itoa(idx) + ":")
		if !i.occupied {
			p.WriteString("\t-\n")
		} else {
			at := m.hash(i.key) % uint64(len(m.items))
			if i.dist > 0 {
				p.WriteString(fmt.Sprintf("^%d", at))
			}
			p.WriteString("\t" + strings.Repeat(".", int(i.dist)) + fmt.Sprintf("%v\n", i.key))
		}
	}
	return p.String()
}
