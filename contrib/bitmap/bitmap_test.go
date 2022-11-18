package bitmap

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/coyove/sdss/contrib/clock"
)

func TestBitmap(t *testing.T) {
	now := clock.Unix() / halfday * halfday
	rand.Seed(now)
	b := New(now)

	var xx []uint32
	var tmp []uint32
	for t := 0; t < halfday; t++ {
		N := 1000
		big := false
		if rand.Intn(5) == 0 {
			N = 10000
			big = true
		}
		for i := 0; i < N; i++ {
			v := rand.Uint32()
			b.Add(now+int64(t), v)
			xx = append(xx, v)
			if big {
				tmp = append(tmp, v)
			}
		}
	}

	start := clock.Now()
	x := b.MarshalBinary(now + 10000)
	fmt.Println(len(x), time.Since(start), b)

	b, _ = UnmarshalBinary(x)

	for t := 10000; t < 15000; t++ {
		for i := 0; i < 10000; i++ {
			v := rand.Uint32()
			b.Add(now+int64(t), v)
		}
	}

	start = clock.Now()
	x = b.MarshalBinary(now + 20000)
	fmt.Println(len(x), time.Since(start), b)

	start = clock.Now()
	x = b.MarshalBinary(now + halfday)
	fmt.Println(len(x), time.Since(start), b)

	b, _ = UnmarshalBinary(x)

	start = clock.Now()
	res := b.Merge(tmp[0:1])
	fmt.Println(time.Since(start))

	res.Iterate(func(ts int64) bool {
		// fmt.Println(ts)
		return true
	})
}
