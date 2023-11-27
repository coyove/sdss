package plru

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
	_ "unsafe"
)

func init() {
	rand.Seed(time.Now().Unix())
}

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
	// fmt.Println(m)
}

func TestRHMapInt64a(t *testing.T) {
	const N = 1e2
	m := NewMap[int64, int](N, Hash.Int64a)

	for i := 0; i < N*2; i++ {
		if rand.Intn(10) > 0 {
			m.Set(rand.Int63(), i+1)
		} else {
			m.Set(int64(i), i+1)
		}
	}

	fmt.Println(m.GoString())
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
		v2, _ := m.Find(k)
		if v2 != v {
			t.Fatal(k)
		}
	}

	fmt.Println(m.density())
	// fmt.Println(m)
}

const BN = 1e6

func BenchmarkGoMapRandomGet(b *testing.B) {
	b.StopTimer()
	m2 := map[int]int{}

	for i := 0; i < BN; i++ {
		m2[i] = i + 1
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		x := rand.Intn(BN * 2)
		if m2[x] == -1 {
			b.Fatal(i)
		}
	}
}

func BenchmarkRHMapRandomGet(b *testing.B) {
	b.StopTimer()
	m := NewMap[int, int](BN, Hash.Int)

	for i := 0; i < BN; i++ {
		m.Set(i, i+1)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		x := rand.Intn(BN * 2)
		if v, _ := m.Find(x); v == -1 {
			b.Fatal(i)
		}
	}
}

func BenchmarkRHMapInt64aRandomGet(b *testing.B) {
	b.StopTimer()
	m := NewMap[int64, int](BN, Hash.Int64a)

	for i := 0; i < BN; i++ {
		m.Set(int64(i), i+1)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		x := rand.Int63n(BN * 2)
		if v, _ := m.Find(x); v == -1 {
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

func BenchmarkRHMapFirstNext(b *testing.B) {
	b.StopTimer()
	m := NewMap[int, int](8, Hash.Int)
	for i := 0; i < 100; i++ {
		m.Set(i, i+1)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c := 0
		for iter := m.First(); iter != nil; iter = m.Next(iter) {
			c++
		}
		if c != m.Len() {
			b.Fail()
		}
	}
}

func BenchmarkRHMapForeach(b *testing.B) {
	b.StopTimer()
	m := NewMap[int, int](8, Hash.Int)
	for i := 0; i < 100; i++ {
		m.Set(i, i+1)
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c := 0
		m.Foreach(func(k int, v *int) bool {
			c++
			return true
		})
		if c != m.Len() {
			b.Fail()
		}
	}
}

func BenchmarkGoMapForeach(b *testing.B) {
	b.StopTimer()
	m := map[int]int{}
	for i := 0; i < 100; i++ {
		m[i] = i + 1
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c := 0
		for range m {
			c++
		}
		if c != len(m) {
			b.Fail()
		}
	}
}
