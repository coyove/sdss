package plru

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
	_ "unsafe"
)

func TestPLRU(t *testing.T) {
	N := 10000
	c := New[int, int](N, Hash.Int, nil)
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

func TestRHMapFixedFull(t *testing.T) {
	const N = 1000
	m := NewMap[int, int](N*0.5, Hash.Int)
	m.Fixed = true

	start := time.Now()
	for i := 0; i < N; i++ {
		m.Set(i, i+1)
	}
	fmt.Println(time.Since(start))
	fmt.Println(m)
}

func TestRHMap(t *testing.T) {
	const N = 1e6
	m2 := map[string]int{}
	m := NewMap[string, int](N*0.5, Hash.Str)

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

const BN = 1e6

func BenchmarkGoMap(b *testing.B) {
	b.StopTimer()
	m2 := map[int]int{}

	for i := 0; i < BN; i++ {
		m2[i] = i + 1
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		x := rand.Intn(BN)
		if m2[x] == 0 {
			b.Fatal(i)
		}
	}
}

func BenchmarkRHMap(b *testing.B) {
	b.StopTimer()
	m := NewMap[int, int](BN, Hash.Int)

	for i := 0; i < BN; i++ {
		m.Set(i, i+1)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		x := rand.Intn(BN)
		if v, _ := m.Get(x); v == 0 {
			b.Fatal(i)
		}
	}
}

func BenchmarkGoMapAddPrealloc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m2 := make(map[int]int, BN/10)
		for i := 0; i < BN/10; i++ {
			m2[i] = i + 1
		}
	}
}

func BenchmarkRHMapAddPrealloc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := NewMap[int, int](BN/10, Hash.Int)
		for i := 0; i < BN/10; i++ {
			m.Set(i, i+1)
		}
	}
}

func BenchmarkGoMapAdd(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m2 := map[int]int{}
		for i := 0; i < BN/10; i++ {
			m2[i] = i + 1
		}
	}
}

func BenchmarkRHMapAdd(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := NewMap[int, int](8, Hash.Int)
		for i := 0; i < BN/10; i++ {
			m.Set(i, i+1)
		}
	}
}
