package cursor

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/FastFilter/xorfilter"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/coyove/sdss/contrib/clock"
)

func TestCursor(t *testing.T) {
	c := New()
	for i := 0; i < 500; i++ {
		c.Add(strconv.Itoa(i))
	}

	hits, misses := 0, 0
	for i := 0; i < 1000; i++ {
		if c.Contains(strconv.Itoa(i)) {
			hits++
		} else {
			misses++
		}
	}
	fmt.Println(hits, misses)

	for i := 500; i < 520; i++ {
		c.Add(strconv.Itoa(i))
	}
	fmt.Println(c.Contains("1"), c.Contains("510"))

	x := c.String()
	fmt.Println(len(x), len(strconv.Quote(x)), c.GoString())
	c, _ = Parse(x)
	for i := 1000; i < 2501; i++ {
		c.Add(strconv.Itoa(i))
	}
	fmt.Println(len(c.String()), c.GoString())

	for i := 1000; i < 2501; i++ {
		c.Add(strconv.Itoa(int(rand.Uint32())))
	}
	for i := 0; i < 580; i++ {
		c.Add(strconv.Itoa(int(rand.Uint32())))
	}
	x = c.String()
	c, _ = Parse(x)
	fmt.Println(len(c.String()), c.GoString())

	{
		mm := bloom.NewWithEstimates(1e3, 1e-5)
		buf, _ := mm.GobEncode()
		fmt.Println(len(buf) * 1000)

		x := []uint64{}
		for i := 0; i < 1e3*2; i++ {
			x = append(x, rand.Uint64())
		}
		xf, _ := xorfilter.Populate(x)
		fmt.Println(len(xf.Fingerprints) * 1000)
	}
}

func TestCursor2(t *testing.T) {
	c := New()
	for i := 0; i < 200; i++ {
		c.Add(strconv.Itoa(i))
	}
	fmt.Println(len(c.MarshalBinary()))
}

func BenchmarkEncode(b *testing.B) {
	rand.Seed(clock.Unix())
	c, _ := Parse("")
	for i := 1000; i < 2501; i++ {
		c.Add(strconv.Itoa(int(rand.Uint32())))
	}
	for i := 0; i < b.N; i++ {
		c.String()
	}
}
