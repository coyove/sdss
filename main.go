package sdss

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unsafe"
)

type Map struct {
	count  int32
	keys   []uint64
	scores []float64
	dists  []uint32
}

func NewMap(size int) *Map {
	obj := &Map{}
	obj.Init(size)
	return obj
}

func calcSize(count int) int {
	n := count * 3 / 2 // allocate 50% more space
	if n*8 > 4096 {
		m := n * 8 / 4096
		n = (m + 1) * 4096 / 8
	}
	return n
}

// Init pre-allocates enough memory for 'count' key and clears all old data.
func (m *Map) Init(count int) *Map {
	if count > 0 {
		n := calcSize(count)
		m.count = 0
		m.keys = make([]uint64, n)
		m.scores = make([]float64, n)
		m.dists = make([]uint32, n)
	}
	return m
}

// Cap returns the capacity of the map in terms of key-value pairs, one pair is (ValueSize * 2 + 8) bytes.
func (m *Map) Cap() int {
	return len(m.keys)
}

// Len returns the count of keys in the map.
func (m *Map) Len() int {
	return int(m.count)
}

// Clear clears all keys in the map, where already allocated memory will be reused.
func (m *Map) Clear() {
	for i := range m.keys {
		m.keys[i], m.scores[i], m.dists[i] = 0, 0, 0
	}
	m.count = 0
}

// Get retrieves the value by 'k', returns false as the second argument if not found.
func (m *Map) Get(k uint64) (v float64, exists bool) {
	if idx := m.findValue(k); idx >= 0 {
		return m.scores[idx], true
	}
	return 0, false
}

func (m *Map) Shrink() {
	if m.count == 0 {
		*m = Map{}
		return
	}

	if calcSize(int(m.count)) >= len(m.keys) {
		return
	}

	tmp := Map{}
	tmp.Init(int(m.count))
	for i := range m.keys {
		if m.dists[i] != 0 {
			tmp.setHash(m.keys[i], m.scores[i])
		}
	}
	*m = tmp
}

func (m *Map) findValue(k uint64) int {
	num := len(m.keys)
	if num <= 0 || k == 0 {
		return -1
	}
	idx := int(hash64(k) % uint32(num))
	idxStart := idx

	for {
		if m.dists[idx] == 0 {
			return -1
		}
		if m.keys[idx] == k {
			return idx
		}
		idx = (idx + 1) % num
		if idx == idxStart {
			return -1
		}
	}
}

// Contains returns true if the map contains 'k'.
func (m *Map) Contains(k uint64) bool {
	return m.findValue(k) >= 0
}

func (m *Map) Put(k uint64, v float64) (prev float64) {
	if len(m.keys) <= 0 {
		m.Init(4)
	}
	if int(m.count) >= len(m.keys)*4/5 {
		m.grow()
	}
	return m.setHash(k, v)
}

func (m *Map) Delete(k uint64) {
	idx := m.findValue(k)
	if idx < 0 {
		return
	}

	m.set(idx, 0, 0, 0)

	num := len(m.keys)
	for idxStart := idx; ; {
		next := (idx + 1) % num
		if m.dists[next] <= 1 {
			break
		}

		m.set(idx, m.keys[next], m.scores[next], m.dists[next]-1)
		m.set(next, 0, 0, 0)

		idx = next
		if idx == idxStart {
			break
		}
	}
	m.count--
}

func (m *Map) set(i int, k uint64, v float64, d uint32) (ok uint64, ov float64, od uint32) {
	m.keys[i], m.scores[i], m.dists[i], ok, ov, od = k, v, d, m.keys[i], m.scores[i], m.dists[i]
	return
}

func (m *Map) setHash(k uint64, v float64) (prev float64) {
	num := len(m.keys)
	idx := int(hash64(k) % uint32(num))
	dist := uint32(1)

	for idxStart := idx; ; {
		if m.dists[idx] == 0 {
			m.set(idx, k, v, dist)
			m.count++
			return prev
		}

		if m.keys[idx] == k {
			_, prev, _ = m.set(idx, k, v, dist)
			return prev
		}

		// Swap if the incoming item is further from its best idx.
		if m.dists[idx] < dist {
			k, v, dist = m.set(idx, k, v, dist)
		}

		dist++ // One step further away from best idx.
		idx = (idx + 1) % num

		if idx == idxStart {
			panic("object space not enough")
		}
	}
}

func (m *Map) Foreach(f func(uint64, float64) bool) {
	for i := 0; i < len(m.keys); i++ {
		if m.dists[i] > 0 {
			if !f(m.keys[i], m.scores[i]) {
				break
			}
		}
	}
}

func (m *Map) String() string {
	needComma := false
	p := bytes.NewBufferString("{")
	m.Foreach(func(k uint64, v float64) bool {
		if needComma {
			p.WriteString(",")
		}
		fmt.Fprintf(p, "%016x:%f", k, v)
		needComma = true
		return true
	})
	p.WriteString("}")
	return p.String()
}

func (m *Map) grow() {
	tmp := Map{}
	tmp.Init(len(m.keys))
	for i := range m.keys {
		if m.dists[i] != 0 {
			tmp.setHash(m.keys[i], m.scores[i])
		}
	}
	*m = tmp
}

func (m *Map) density() float64 {
	num := len(m.keys)
	if num <= 0 || m.count <= 0 {
		return math.NaN()
	}

	var maxRun int
	for i := 0; i < num; {
		if m.dists[i] == 0 {
			i++
			continue
		}
		run := 1
		for i++; i < num; i++ {
			if m.dists[i] != 0 {
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

func (m *Map) debugString() string {
	p := bytes.Buffer{}
	for idx := range m.keys {
		p.WriteString(strconv.Itoa(idx) + ":")
		if m.dists[idx] == 0 {
			p.WriteString("\t-\n")
		} else {
			at := hash64(m.keys[idx]) % uint32(len(m.keys))
			if m.dists[idx] > 1 {
				p.WriteString(fmt.Sprintf("^%d", at))
			}
			p.WriteString("\t" + strings.Repeat(".", int(m.dists[idx]-1)) + fmt.Sprintf("%v\n", m.keys[idx]))
		}
	}
	return p.String()
}

func (m *Map) Index(i int) (uint64, float64, bool) {
	return m.keys[i], m.scores[i], m.dists[i] > 0
}

//go:linkname memhash64 runtime.memhash64
func memhash64(p unsafe.Pointer, h uintptr) uintptr

func hash64(x uint64) uint32 {
	xp := uintptr(unsafe.Pointer(&x))
	h := memhash64(unsafe.Pointer(xp^0), 0)
	return uint32(h)
}
