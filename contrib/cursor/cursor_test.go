package cursor

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/coyove/sdss/contrib/bitmap"
)

func TestCursor(t *testing.T) {
	for n := 10; n < 1e7; n *= 10 {
		c := New()
		data := []uint32{}
		for i := 0; i < n; i++ {
			v := rand.Uint32()
			data = append(data, v)
			c.Add(bitmap.Uint64Key(uint64(v)))
		}
		buf := c.MarshalBinary()
		fmt.Println(n, len(buf), c.m.GetSerializedSizeInBytes())

		c, _ = Read(bytes.NewReader(buf))
		for _, v := range data {
			if !c.Contains(bitmap.Uint64Key(uint64(v))) {
				panic(v)
			}
		}
	}
}
