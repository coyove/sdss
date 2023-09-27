package plru

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"strconv"
	"testing"
)

func hash(v string) uint64 {
	h := sha1.Sum([]byte(v))
	return binary.BigEndian.Uint64(h[:8])
}

func ihash(v int) uint64 {
	h := sha1.Sum(strconv.AppendInt(nil, int64(v), 10))
	return binary.BigEndian.Uint64(h[:8])
}

func TestPLRU(t *testing.T) {
	N := 10000
	c := New[int, int](N, ihash, nil)
	for i := 0; i < N*2; i++ {
		c.Add(i, i)
	}
	x := 0
	c.Range(func(k, v int) bool {
		if k < N {
			x++
		}
		return true
	})
	fmt.Println(x, N)
}

func TestRHMap(t *testing.T) {
	const N = 1e6
	m2 := map[string]int{}
	m := NewMap[string, int](N*0.5, hash)

	for i := 0; i < N; i++ {
		is := strconv.Itoa(i)
		m2[is] = i + 1
		m.Set(is, i+1)
	}

	for i := 0; i < N/10; i++ {
		for k := range m2 {
			delete(m2, k)
			m.Delete(k)
			break
		}
	}

	for k, v := range m2 {
		v2, _ := m.Get(k)
		if v2 != v {
			t.Fatal(k)
		}
	}

	fmt.Println(m.density())
	// fmt.Println(m)
}
