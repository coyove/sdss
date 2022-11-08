package sdss

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestMap(t *testing.T) {
	rand.Seed(time.Now().Unix())

	const N = 1e5
	m := NewMap(0)
	m2 := map[uint64]float64{}

	for i := 0; i < N; i++ {
		k, v := rand.Uint64(), rand.Float64()
		m.Put(k, v)
		m2[k] = v
	}

	fmt.Println(m.density(), m.Cap(), m.Len())

	for k := range m2 {
		delete(m2, k)
		m.Delete(k)
		if len(m2) <= N*3/4 {
			break
		}
	}

	for i := 0; i < N/4; i++ {
		k, v := rand.Uint64(), rand.Float64()
		m.Put(k, v)
		m2[k] = v
	}

	fmt.Println(m.Save("dump.map"))
	m = &Map{}
	fmt.Println(m.Load("dump.map"))
	fmt.Println(m.density(), m.Cap(), m.Len())

	m.Foreach(func(k uint64, v float64) bool {
		if m2[k] != v {
			t.Fatal()
		}
		return true
	})
	for k, v := range m2 {
		if v2, _ := m.Get(k); v2 != v {
			t.Fatal()
		}
	}
	fmt.Println(m.Len(), len(m2))
}

func BenchmarkMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := NewMap(0)
		for i := 0; i < 1e3; i++ {
			m.Put(uint64(i), float64(i))
		}
	}
}

func BenchmarkGoMap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := map[uint64]float64{}
		for i := 0; i < 1e3; i++ {
			m[uint64(i)] = float64(i)
		}
	}
}
