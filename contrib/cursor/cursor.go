package cursor

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/bits-and-blooms/bloom/v3"
)

var (
	Count         = 200
	FalsePositive = 0.001
)

type Cursor struct {
	Next  int64
	count int64
	bf    [3]*bloom.BloomFilter
}

func Parse(v string) (*Cursor, bool) {
	if v == "" {
		c := &Cursor{}
		for i := range c.bf {
			c.bf[i] = bloom.NewWithEstimates(uint(Count), FalsePositive)
		}
		return c, true
	}

	tmp := []byte(v)
	for i := len(tmp) - 1; i >= 0; i-- {
		idx := strings.IndexByte(compressA, tmp[i])
		if idx >= 0 {
			count := idx + 2
			tmp = append(tmp, decompressA[:count-1]...)
			copy(tmp[i+count:], tmp[i+1:])
			copy(tmp[i:], decompressA[:count])
		}
	}
	// fmt.Println(string(tmp))

	rd := base64.NewDecoder(base64.URLEncoding, bytes.NewReader(tmp))
	// for i, c := range tmp {
	// 	switch c {
	// 	case '{':
	// 		tmp[i] = '\''
	// 	case '}':
	// 		tmp[i] = '"'
	// 	case '|':
	// 		tmp[i] = '\\'
	// 	}
	// }

	// rd := ascii85.NewDecoder(bytes.NewReader(tmp))
	c := &Cursor{}
	if err := binary.Read(rd, binary.BigEndian, &c.Next); err != nil {
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
		tmp := make([]byte, length)
		if err := binary.Read(rd, binary.BigEndian, tmp); err != nil {
			return nil, false
		}
		if err := c.bf[i].GobDecode(tmp); err != nil {
			return nil, false
		}
	}
	return c, false
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
	x := fmt.Sprintf("next: %x, count: %d", c.Next, c.count)
	for i, bf := range c.bf {
		x += fmt.Sprintf(", b%d: %d", i, bf.ApproximatedSize())
	}
	return x
}

func (c *Cursor) String() string {
	buf := &bytes.Buffer{}
	out := base64.NewEncoder(base64.URLEncoding, buf)
	binary.Write(out, binary.BigEndian, c.Next)
	binary.Write(out, binary.BigEndian, c.count)

	for _, bf := range c.bf {
		a, _ := bf.GobEncode()
		binary.Write(out, binary.BigEndian, uint32(len(a)))
		binary.Write(out, binary.BigEndian, a)
	}

	out.Close()
	// old := buf.String()
	res := buf.Bytes()

	for i := len(res) - 1; i >= 0; {
		if res[i] == 'A' {
			count := 1
			j := i - 1
			for ; j >= 0; j-- {
				if res[j] == 'A' {
					count++
					if count < 12 {
						continue
					}
				} else {
					j++
				}
				break
			}
			if j < 0 {
				j = 0
			}
			if count > 1 {
				res[j] = compressA[count-2]
				res = append(res[:j+1], res[i+1:]...)
				i = j - 1
				continue
			}
		}
		i--
	}

	// fmt.Println(old)
	return string(res) // buf.String()
}

const (
	compressA   = ".~()'!*:@,;"
	decompressA = "AAAAAAAAAAAAAAA"
)
