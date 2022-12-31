package cursor

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/FastFilter/xorfilter"
	"github.com/coyove/sdss/contrib/bitmap"
	"github.com/coyove/sdss/contrib/clock"
)

func TestCursor(t *testing.T) {
	rand.Seed(clock.Unix())

	{
		c := New()
		what := 0
		for i := 0; i < 1e6; i++ {
			if !c.Add(bitmap.Uint64Key(uint64(i))) {
				what++
			}
		}
		fmt.Println("ok")
		hits := 0
		for i := 0; i < 1e6; i++ {
			// if c.Contains(bitmap.Uint64Key(rand.Uint64())) {
			if ok := c.Contains((bitmap.Uint64Key(rand.Uint64()))); ok {
				// fmt.Println(i, idx)
				hits++
			}
		}
		fmt.Println(hits, what)
	}

	for n := 10; n < 1e7; n *= 8 {
		c := New()
		data := []uint64{}
		dedup := map[uint32]bool{}
		for i := 0; i < n; i++ {
		AGAIN:
			v := rand.Uint32()
			if dedup[v] {
				goto AGAIN
			}
			dedup[v] = true
			data = append(data, uint64(v))
			c.Add(bitmap.Uint64Key(uint64(v)))
		}
		buf := c.MarshalBinary()

		xf, _ := xorfilter.Populate(data)
		fmt.Println(n, len(buf), len(xf.Fingerprints)*2)

		c, _ = Read(bytes.NewReader(buf))
		for _, v := range data {
			if !c.Contains(bitmap.Uint64Key(uint64(v))) {
				panic(v)
			}
		}
	}
}

func BenchmarkAdd(b *testing.B) {
	c := New()
	for i := 0; i < b.N; i++ {
		c.Add(bitmap.Uint64Key(uint64(i)))
	}
}
