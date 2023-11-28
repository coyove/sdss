package plru

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unsafe"
	_ "unsafe"
)

type Map[K comparable, V any] struct {
	Fixed bool

	count uint32
	hash  func(K) uint64
	items []hashItem[K, V]
}

// hashItem represents a slot in the map.
type hashItem[K, V any] struct {
	dist     uint32
	occupied bool
	Key      K
	Value    V
}

func NewMap[K comparable, V any](size int, hash func(K) uint64) *Map[K, V] {
	if size < 1 {
		size = 1
	}
	obj := &Map[K, V]{hash: hash}
	obj.items = make([]hashItem[K, V], size*2)
	return obj
}

// Cap returns the capacity of the map.
// Cap * 0.75 is the expanding threshold for non-fixed map.
// Fixed map panic when keys exceed the capacity.
func (m *Map[K, V]) Cap() int {
	if m == nil {
		return 0
	}
	return len(m.items)
}

// Len returns the count of keys in the map.
func (m *Map[K, V]) Len() int {
	if m == nil {
		return 0
	}
	return int(m.count)
}

// Clear clears all keys in the map, allocated memory will be reused.
func (m *Map[K, V]) Clear() {
	for i := range m.items {
		m.items[i] = hashItem[K, V]{}
	}
	m.count = 0
}

// Find finds the value by 'k', returns false as the second argument if not found.
func (m *Map[K, V]) Find(k K) (v V, exists bool) {
	if m == nil {
		return
	}
	if idx := m.findValue(k); idx >= 0 {
		return m.items[idx].Value, true
	}
	return v, false
}

// Get gets the value by 'k'.
func (m *Map[K, V]) Get(k K) (v V) {
	if m == nil {
		return
	}
	if idx := m.findValue(k); idx >= 0 {
		return m.items[idx].Value
	}
	return v
}

// Ref retrieves the value pointer by 'k', it is legal to alter what it points to
// as long as the map stays unchanged.
func (m *Map[K, V]) Ref(k K) (v *V) {
	if m == nil {
		return nil
	}
	if idx := m.findValue(k); idx >= 0 {
		return &m.items[idx].Value
	}
	return nil
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

		if e.Key == k {
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
	if m == nil {
		return false
	}
	return m.findValue(k) >= 0
}

// Set upserts a key-value pair in the map and returns the previous value if updated.
func (m *Map[K, V]) Set(k K, v V) (prev V, updated bool) {
	if len(m.items) <= 0 {
		m.items = make([]hashItem[K, V], 8)
	}
	if int(m.count) >= len(m.items)*3/4 {
		m.resizeHash(len(m.items) * 2)
	}
	return m.setHash(hashItem[K, V]{Key: k, Value: v, occupied: true})
}

// Delete deletes a key from the map, returns deleted value if existed.
func (m *Map[K, V]) Delete(k K) (prev V, ok bool) {
	idx := m.findValue(k)
	if idx < 0 {
		return prev, false
	}
	prev = m.items[idx].Value

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

func (m *Map[K, V]) setHash(incoming hashItem[K, V]) (prev V, updated bool) {
	num := len(m.items)
	idx := int(m.hash(incoming.Key) % uint64(num))

	for idxStart := idx; ; {
		e := &m.items[idx]

		if !e.occupied {
			m.items[idx] = incoming
			m.count++
			return
		}

		if e.Key == incoming.Key {
			prev = e.Value
			e.Value, e.dist = incoming.Value, incoming.dist
			return prev, true
		}

		// Swap if the incoming item is further from its best idx.
		if e.dist < incoming.dist {
			incoming, m.items[idx] = m.items[idx], incoming
		}

		incoming.dist++ // one step further away from best idx.
		idx = (idx + 1) % num

		if idx == idxStart {
			if m.Fixed {
				panic("fixed map is full")
			} else {
				panic("fatal: space not enough")
			}
		}
	}
}

// Foreach iterates all keys in the map, for each of them, 'f(key, &value)' will be
// called. Values are passed by pointers and it is legal to manipulate them directly in 'f'.
func (m *Map[K, V]) Foreach(f func(K, *V) bool) {
	if m == nil {
		return
	}
	for i := 0; i < len(m.items); i++ {
		ip := &m.items[i]
		if ip.occupied {
			if !f(ip.Key, &ip.Value) {
				return
			}
		}
	}
}

// Keys returns all keys in the map as list.
func (m *Map[K, V]) Keys() (res []K) {
	if m == nil {
		return
	}
	for i := 0; i < len(m.items); i++ {
		ip := &m.items[i]
		if ip.occupied {
			res = append(res, ip.Key)
		}
	}
	return
}

// Values returns all values in the map as list.
func (m *Map[K, V]) Values() (res []V) {
	if m == nil {
		return
	}
	for i := 0; i < len(m.items); i++ {
		ip := &m.items[i]
		if ip.occupied {
			res = append(res, ip.Value)
		}
	}
	return
}

func (m *Map[K, V]) nextItem(idx int) (int, *hashItem[K, V]) {
	for i := idx; i < len(m.items); i++ {
		if p := &m.items[i]; p.occupied {
			return i, p
		}
	}
	return 0, nil
}

func (m *Map[K, V]) First() *hashItem[K, V] {
	if m == nil {
		return nil
	}
	for i := range m.items {
		if m.items[i].occupied {
			return &m.items[i]
		}
	}
	return nil
}

func (m *Map[K, V]) Next(el *hashItem[K, V]) *hashItem[K, V] {
	if len(m.items) == 0 {
		return nil
	}
	hashItemSize := unsafe.Sizeof(hashItem[K, V]{})
	for el != &m.items[len(m.items)-1] {
		ptr := uintptr(unsafe.Pointer(el)) + hashItemSize
		el = (*hashItem[K, V])(unsafe.Pointer(ptr))
		if el.occupied {
			return el
		}
	}
	return nil
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
	p, f := bytes.NewBufferString("{"), false
	for _, i := range m.items {
		if !i.occupied {
			continue
		}
		fmt.Fprintf(p, "%v: %v, ", i.Key, i.Value)
		f = true
	}
	if f {
		p.Truncate(p.Len() - 2)
	}
	p.WriteString("}")
	return p.String()
}

func (m *Map[K, V]) GoString() string {
	w := "                "[:int(math.Ceil(math.Log10(float64(len(m.items)))))]
	itoa := func(i int) string {
		s := strconv.Itoa(i)
		return w[:len(w)-len(s)] + s
	}
	p := bytes.Buffer{}
	var maxDist uint32
	for idx, i := range m.items {
		p.WriteString(itoa(idx) + ":")
		if !i.occupied {
			p.WriteString(w)
			p.WriteString(" \t-\n")
		} else {
			at := m.hash(i.Key) % uint64(len(m.items))
			if i.dist > 0 {
				p.WriteString("^")
				p.WriteString(itoa(int(at)))
				if i.dist > uint32(maxDist) {
					maxDist = i.dist
				}
			} else {
				p.WriteString(w)
				p.WriteString(" ")
			}
			p.WriteString("\t" + strings.Repeat(".", int(i.dist)) + fmt.Sprintf("%v\n", i.Key))
		}
	}
	fmt.Fprintf(&p, "max distance: %d", maxDist)
	return p.String()
}
