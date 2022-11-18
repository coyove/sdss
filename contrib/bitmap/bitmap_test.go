package bitmap

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"math/rand"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/coyove/sdss/contrib/clock"
)

func TestBitmap(t *testing.T) {
	now := clock.Unix()
	b := New(now)
	m := roaring.New()
	m2 := roaring.New()

	for t := 0; t < 3000; t++ {
		for i := 0; i < 1e3; i++ {
			v := rand.Uint32()
			b.Add(now+int64(t), v&0xfffff)
			m.Add(uint32(t)<<16 | v&0xffff)
			m2.Add(uint32(t)<<16 | (v>>16)&0xffff)
		}
	}
	x := b.MarshalBinary()

	tmp := &bytes.Buffer{}
	{
		out := gzip.NewWriter(tmp)
		buf, _ := m.MarshalBinary()
		out.Write(buf)
		out.Flush()
	}
	fmt.Println(len(x), m.GetSerializedSizeInBytes()+m2.GetSerializedSizeInBytes(), tmp.Len())
	return

	b, _ = FromBinary(x)
	fmt.Println(b.MergeTimestamps([]uint32{1, 10, 9}), len(x))
}
