package cursor

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/pierrec/lz4"
)

var (
	Count         = 1024
	FalsePositive = 1e-6
)

type Cursor struct {
	NextMap int64
	NextId  int64
	count   int64
	bf      [16]*bloom.BloomFilter
}

func New() *Cursor {
	c := &Cursor{}
	for i := range c.bf {
		c.bf[i] = bloom.NewWithEstimates(uint(Count), FalsePositive)
	}
	return c
}

func Parse(v string) (*Cursor, bool) {
	return Read(base64.NewDecoder(base64.URLEncoding, strings.NewReader(v)))
}

func Read(rd io.Reader) (*Cursor, bool) {
	rd = lz4.NewReader(rd)
	c := &Cursor{}
	if err := binary.Read(rd, binary.BigEndian, &c.NextMap); err != nil {
		return nil, false
	}
	if err := binary.Read(rd, binary.BigEndian, &c.NextId); err != nil {
		return nil, false
	}
	if err := binary.Read(rd, binary.BigEndian, &c.count); err != nil {
		return nil, false
	}

	for i := range c.bf {
		c.bf[i] = &bloom.BloomFilter{}
		var length uint32
		if err := binary.Read(rd, binary.BigEndian, &length); err != nil {
			return nil, false
		}
		if length == 0 {
			c.bf[i] = bloom.NewWithEstimates(uint(Count), FalsePositive)
		} else {
			if _, err := c.bf[i].ReadFrom(io.LimitReader(rd, int64(length))); err != nil {
				return nil, false
			}
		}
	}
	return c, true
}

func (c *Cursor) Add(key string) {
	idx := c.count / int64(Count)
	lastIdx := (c.count - 1) / int64(Count)

	i := idx % int64(len(c.bf))
	if lastIdx != idx {
		c.bf[i].ClearAll()
	}
	c.bf[i].AddString(key)
	c.count++
}

func (c *Cursor) Contains(key string) bool {
	for _, bf := range c.bf {
		if bf.TestString(key) {
			return true
		}
	}
	return false
}

func (c *Cursor) GoString() string {
	x := fmt.Sprintf("next: %x-%x, count: %d", c.NextMap, c.NextId, c.count)
	for i, bf := range c.bf {
		x += fmt.Sprintf(", b%d: %d", i, bf.ApproximatedSize())
	}
	return x
}

func (c *Cursor) MarshalBinary() []byte {
	buf := &bytes.Buffer{}
	out := lz4.NewWriter(buf)
	binary.Write(out, binary.BigEndian, c.NextMap)
	binary.Write(out, binary.BigEndian, c.NextId)
	binary.Write(out, binary.BigEndian, c.count)

	mapIdx := (c.count / int64(Count)) % int64(len(c.bf))
	for i, bf := range c.bf {
		if c.count < int64(Count*len(c.bf)) {
			if i > int(mapIdx) {
				binary.Write(out, binary.BigEndian, uint32(0))
				continue
			}
		}
		a, _ := bf.GobEncode()
		binary.Write(out, binary.BigEndian, uint32(len(a)))
		binary.Write(out, binary.BigEndian, a)
	}
	out.Close()
	return buf.Bytes()
}

// old := buf.String()
func (c *Cursor) String() string {
	return base64.URLEncoding.EncodeToString(c.MarshalBinary())
}
