package bitmap

import (
	"bytes"
	"fmt"
	"io"
	"math/bits"
	"os"
	"time"

	"github.com/coyove/sdss/contrib/clock"
)

type Values struct {
	Oneof []uint64
	Major []uint64
	Exact []uint64
}

type meterWriter struct {
	io.Writer
	size int
}

func (m *meterWriter) Close() error {
	return nil
}

func (m *meterWriter) Write(p []byte) (int, error) {
	n, err := m.Writer.Write(p)
	m.size += n
	return n, err
}

func h16(v uint32, ts int64) (out [4]uint32) {
	const mask = 0xffffffff
	out[0] = combinehash(v, uint32(ts)) & mask
	out[1] = combinehash(v, out[0]) & mask
	out[2] = combinehash(v, out[1]) & mask
	out[3] = combinehash(v, out[2]) & mask
	return
}

func icmp(a, b int64) int {
	if a > b {
		return 1
	}
	if a < b {
		return -1
	}
	return 0
}

func h32(index uint32, hash uint32) (out uint32) {
	return index<<17 | hash&0x1ffff
}

func combinehash(k1, seed uint32) uint32 {
	h1 := seed

	k1 *= 0xcc9e2d51
	k1 = bits.RotateLeft32(k1, 15)
	k1 *= 0x1b873593

	h1 ^= k1
	h1 = bits.RotateLeft32(h1, 13)
	h1 = h1*4 + h1 + 0xe6546b64

	h1 ^= uint32(4)

	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}

type KeyIdScore struct {
	Key   Key
	Id    int64
	Score int
}

type JoinMetrics struct {
	BaseStart   int64
	Start       int64
	Desc        bool
	Values      Values
	FastElapsed time.Duration
	Elapsed     time.Duration
	Slots       [slotNum]struct {
		Scans, Hits int
		Elapsed     time.Duration
	}
}

func (jm JoinMetrics) String() string {
	dir := "asc"
	if jm.Desc {
		dir = "desc"
	}
	x := fmt.Sprintf("join map [%d] start at %d (%s) in %vus",
		jm.BaseStart, jm.Start, dir, jm.Elapsed.Microseconds())
	x += fmt.Sprintf("\n\tinput: oneof=%d, major=%d (min=%d), exact=%d",
		len(jm.Values.Oneof), len(jm.Values.Major), jm.Values.majorScore(), len(jm.Values.Exact))
	x += fmt.Sprintf("\n\tfast lookup: %vus", jm.FastElapsed.Microseconds())

	c := 0
	for i := len(jm.Slots) - 1; i >= 0; i-- {
		s := jm.Slots[i]
		if s.Scans > 0 {
			x += fmt.Sprintf("\n\tsubrange [%02d]: %4dus, %d out of %d",
				i, s.Elapsed.Microseconds(), s.Hits, s.Scans)
			if s.Hits == 0 {
				x += " (NO HITS)"
			}
			c++
		}
	}
	if c == 0 {
		x += " (NO HITS)"
	}
	return x
}

func (b *Range) Save(path string, compress bool) (int, error) {
	b.mfmu.Lock()
	defer b.mfmu.Unlock()

	bakpath := fmt.Sprintf("%s.%d.mtfbak", path, clock.Unix())

	f, err := os.Create(bakpath)
	if err != nil {
		return 0, err
	}
	sz, err := b.Marshal(f, compress)
	f.Close()
	if err != nil {
		return 0, err
	}

	if err := os.Remove(path); err != nil {
		if !os.IsNotExist(err) {
			return 0, err
		}
	}
	return sz, os.Rename(bakpath, path)
}

func Load(path string) (*Range, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	return Unmarshal(f)
}

type bitmap1024 [fastSlotNum / 64]uint64

func (b *bitmap1024) add(index uint16) { // [0, fastSlotNum)
	(*b)[index/64] |= 1 << (index % 64)
}

func (b *bitmap1024) contains(index uint16) bool {
	return (*b)[index/64]&(1<<(index%64)) > 0
}

func (b *bitmap1024) iterate(f func(uint16) bool) {
	for si, s := range *b {
		for i := 0; i < 64; i++ {
			if s&(1<<i) > 0 {
				if !f(uint16(si*64 + i)) {
					return
				}
			}
		}
	}
}

func (b *bitmap1024) and(b2 *bitmap1024) {
	for i := range *b {
		(*b)[i] &= (*b2)[i]
	}
}

func (b *bitmap1024) or(b2 *bitmap1024) {
	for i := range *b {
		(*b)[i] |= (*b2)[i]
	}
}

func (b bitmap1024) String() string {
	buf := bytes.NewBufferString("{fast bitmap")
	for i := 0; i < fastSlotNum; i++ {
		if b.contains(uint16(i)) {
			fmt.Fprintf(buf, " %d", i)
		}
	}
	buf.WriteString("}")
	return buf.String()
}

func (vs *Values) majorScore() int {
	s := len(vs.Major)
	if s <= 2 {
		return s
	}
	if s <= 4 {
		return s - 1
	}
	return s * 4 / 5
}

func (v *Values) clean() {
	m := map[uint64]byte{}

	add := func(a []uint64, typ byte) {
		for _, v := range a {
			m[v] = typ
		}
	}
	remove := func(a []uint64, typ byte) []uint64 {
		for i := len(a) - 1; i >= 0; i-- {
			if m[a[i]] != typ {
				a = append(a[:i], a[i+1:]...)
			}
		}
		return a
	}

	add(v.Oneof, 'o')
	add(v.Major, 'm')
	add(v.Exact, 'e')

	v.Oneof = remove(v.Oneof, 'o')
	v.Major = remove(v.Major, 'm')
	v.Exact = remove(v.Exact, 'e')
}
