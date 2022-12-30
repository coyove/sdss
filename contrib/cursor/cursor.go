package cursor

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"strings"

	"github.com/coyove/sdss/contrib/bitmap"
	"github.com/coyove/sdss/contrib/roaring"
)

type Cursor struct {
	NextMap int64
	NextId  int64
	m       *roaring.Bitmap
}

func New() *Cursor {
	c := &Cursor{}
	c.m = roaring.New()
	return c
}

func Parse(v string) (*Cursor, bool) {
	return Read(base64.NewDecoder(base64.URLEncoding, strings.NewReader(v)))
}

func Read(rd io.Reader) (*Cursor, bool) {
	c := &Cursor{}
	if err := binary.Read(rd, binary.BigEndian, &c.NextMap); err != nil {
		return nil, false
	}
	if err := binary.Read(rd, binary.BigEndian, &c.NextId); err != nil {
		return nil, false
	}

	c.m = roaring.New()

	var typ byte
	if err := binary.Read(rd, binary.BigEndian, &typ); err != nil {
		return nil, false
	}
	if typ == 0 {
		for {
			var d uint32
			if err := binary.Read(rd, binary.BigEndian, &d); err != nil {
				if err == io.EOF {
					break
				}
				return nil, false
			}
			c.m.Add(d)
		}
	} else {
		if _, err := c.m.ReadFrom(rd); err != nil {
			return nil, false
		}
	}
	return c, true
}

func (c *Cursor) Add(key bitmap.Key) {
	h := hashCode(key)
	c.m.Add(h[0])
	c.m.Add(h[1])
}

func (c *Cursor) Contains(key bitmap.Key) bool {
	h := hashCode(key)
	return c.m.Contains(h[0]) && c.m.Contains(h[1])
}

func (c *Cursor) GoString() string {
	x := fmt.Sprintf("next: %x-%x, count: %d", c.NextMap, c.NextId, c.m.GetCardinality())
	return x
}

func (c *Cursor) shouldLinear() bool {
	card := c.m.GetCardinality()
	if card == 0 {
		return true
	}
	return c.m.GetSerializedSizeInBytes()/(card/2) > 8
}

func (c *Cursor) GetMarshalSize() int {
	sz := 8 + 8 + 1
	if c.shouldLinear() {
		sz += int(c.m.GetCardinality()) * 4
	} else {
		sz += int(c.m.GetSerializedSizeInBytes())
	}
	return sz
}

func (c *Cursor) MarshalBinary() []byte {
	out := &bytes.Buffer{}
	binary.Write(out, binary.BigEndian, c.NextMap)
	binary.Write(out, binary.BigEndian, c.NextId)
	if c.shouldLinear() {
		out.WriteByte(0)
		c.m.Iterate(func(x uint32) bool {
			binary.Write(out, binary.BigEndian, x)
			return true
		})
	} else {
		out.WriteByte(1)
		c.m.WriteTo(out)
	}
	return out.Bytes()
}

// old := buf.String()
func (c *Cursor) String() string {
	return base64.URLEncoding.EncodeToString(c.MarshalBinary())
}

func hashCode(k bitmap.Key) [2]uint32 {
	a := crc32.ChecksumIEEE(k[:])
	for i := range k {
		k[i] = ^k[i]
	}
	b := crc32.ChecksumIEEE(k[:])
	return [2]uint32{a, b}
}
