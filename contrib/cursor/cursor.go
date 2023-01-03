package cursor

import (
	"bytes"
	"encoding/ascii85"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"sync"
	"unsafe"

	"github.com/FastFilter/xorfilter"
	"github.com/coyove/sdss/contrib/bitmap"
)

const compactBFHash = 3

var (
	compactThreshold = []int{128, 256, 512, 1024, 2048, 4096, 8192}
	compactBytesSize = func() (a []int) {
		for i := range compactThreshold {
			bf := compactBFHash
			capacity := 32 + uint32(math.Ceil(1.23*float64(compactThreshold[i]*bf)))
			capacity = capacity / 3 * 3 // round it down to a multiple of 3
			a = append(a, int(capacity))
		}
		return
	}()
)

func at(a []int, idx int) int {
	if idx < len(a) {
		return a[idx]
	}
	return a[len(a)-1]
}

type Cursor struct {
	PrevMap int64
	PrevId  int64
	NextMap int64
	NextId  int64

	pendings []uint64
	compacts []xorfilter.Xor8

	_dedup map[uint64]struct{}
	_mu    sync.RWMutex
}

func New() *Cursor {
	c := &Cursor{}
	c._dedup = map[uint64]struct{}{}
	return c
}

func Parse(buf []byte) (*Cursor, bool) {
	for i, b := range buf {
		switch b {
		case '~':
			buf[i] = '\\'
		case '{':
			buf[i] = '"'
		case '}':
			buf[i] = '\''
		}
	}
	return Read(ascii85.NewDecoder(bytes.NewReader(buf)))
}

func Read(rd io.Reader) (*Cursor, bool) {
	c := &Cursor{}
	if err := binary.Read(rd, binary.BigEndian, &c.PrevMap); err != nil {
		return nil, false
	}
	if err := binary.Read(rd, binary.BigEndian, &c.PrevId); err != nil {
		return nil, false
	}
	if err := binary.Read(rd, binary.BigEndian, &c.NextMap); err != nil {
		return nil, false
	}
	if err := binary.Read(rd, binary.BigEndian, &c.NextId); err != nil {
		return nil, false
	}

	var pendingsCount uint16
	if err := binary.Read(rd, binary.BigEndian, &pendingsCount); err != nil {
		return nil, false
	}

	c.pendings = make([]uint64, pendingsCount)
	c._dedup = map[uint64]struct{}{}
	if err := binary.Read(rd, binary.BigEndian, c.pendings); err != nil {
		return nil, false
	}
	for _, p := range c.pendings {
		c._dedup[p] = struct{}{}
	}

	var compactsCount uint16
	if err := binary.Read(rd, binary.BigEndian, &compactsCount); err != nil {
		return nil, false
	}

	c.compacts = make([]xorfilter.Xor8, compactsCount)
	tmp := make([]byte, compactBytesSize[len(compactBytesSize)-1]+8)
	for i := range c.compacts {
		sz := at(compactBytesSize, i)
		tmp = tmp[:sz+8]
		if err := binary.Read(rd, binary.BigEndian, tmp); err != nil {
			return nil, false
		}
		c.compacts[i].BlockLength = uint32(sz) / 3
		c.compacts[i].Seed = binary.BigEndian.Uint64(tmp[:8])
		c.compacts[i].Fingerprints = append([]byte{}, tmp[8:]...)
	}
	return c, true
}

func (c *Cursor) clearDedup() {
	for k := range c._dedup {
		delete(c._dedup, k)
	}
}

func (c *Cursor) Add(key bitmap.Key) bool {
	c._mu.Lock()
	defer c._mu.Unlock()

	h := hashCode(key)
	_, exist := c.contains(h, expandHash(h))
	if exist {
		return false
	}

	c.pendings = append(c.pendings, h)
	c._dedup[h] = struct{}{}

	if len(c.pendings) == at(compactThreshold, len(c.compacts)) {
		bf := compactBFHash
		tmp := make([]uint64, 0, len(c.pendings)*bf)
		c.clearDedup()
		for _, p := range c.pendings {
			h := expandHash(p)
			for i := 0; i < bf; i++ {
				for {
					if _, ok := c._dedup[h[i]]; ok {
						h[i]++
					} else {
						break
					}
				}
				tmp = append(tmp, h[i])
				c._dedup[h[i]] = struct{}{}
			}
		}
		xf, _ := xorfilter.Populate(tmp)
		c.pendings = c.pendings[:0]
		c.compacts = append(c.compacts, *xf)
		c.clearDedup()
	}
	return true
}

func (c *Cursor) Contains(key bitmap.Key) bool {
	c._mu.RLock()
	defer c._mu.RUnlock()
	h := hashCode(key)
	_, ok := c.contains(h, expandHash(h))
	return ok
}

func (c *Cursor) contains(h uint64, bfh [compactBFHash]uint64) (int, bool) {
	if _, ok := c._dedup[h]; ok {
		return -1, true
	}

NEXT:
	for i, cp := range c.compacts {
		bf := compactBFHash //  at(compactBFHash, i)
		for i := 0; i < bf; i++ {
			if !cp.Contains(bfh[i]) {
				continue NEXT
			}
		}
		return i, true
	}
	return -1, false
}

func (c *Cursor) GoString() string {
	x := fmt.Sprintf("next: %x-%x, pendings: %d", c.NextMap, c.NextId, len(c.pendings))
	return x
}

func (c *Cursor) MarshalBinary() []byte {
	out := &bytes.Buffer{}
	binary.Write(out, binary.BigEndian, c.PrevMap)
	binary.Write(out, binary.BigEndian, c.PrevId)
	binary.Write(out, binary.BigEndian, c.NextMap)
	binary.Write(out, binary.BigEndian, c.NextId)
	binary.Write(out, binary.BigEndian, uint16(len(c.pendings)))
	binary.Write(out, binary.BigEndian, c.pendings)
	binary.Write(out, binary.BigEndian, uint16(len(c.compacts)))
	for _, cp := range c.compacts {
		binary.Write(out, binary.BigEndian, cp.Seed)
		binary.Write(out, binary.BigEndian, cp.Fingerprints)
	}
	return out.Bytes()
}

func (c *Cursor) String() string {
	buf := &bytes.Buffer{}
	w := ascii85.NewEncoder(buf)
	w.Write(c.MarshalBinary())
	w.Close()
	for i, b := range buf.Bytes() {
		switch b {
		case '\\':
			buf.Bytes()[i] = '~'
		case '"':
			buf.Bytes()[i] = '{'
		case '\'':
			buf.Bytes()[i] = '}'
		}
	}
	return buf.String()
}

func hashCode(k bitmap.Key) uint64 {
	a := *(*[2]uint64)(unsafe.Pointer(&k))
	return hash2(a[0], a[1])
}

func hash2(a, b uint64) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	h ^= a
	h *= prime64
	h ^= b
	h *= prime64
	return h
}

func expandHash(h uint64) (a [compactBFHash]uint64) {
	a[0] = h
	a[1] = ^h
	a[2] = bits.ReverseBytes64(h)
	return
}
