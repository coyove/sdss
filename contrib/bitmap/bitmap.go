package bitmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
)

const (
	hour10 = 3600 * 10
	day10  = 86400 * 10
)

var andTable [hour10]*roaring.Bitmap

func init() {
	for i := 0; i < hour10; i++ {
		andTable[i] = roaring.New()
		andTable[i].AddRange(uint64(i)<<16, uint64(i)<<16+65536)
	}
}

type Day struct {
	baseTime int64
	hours    [24]*hourMap
}

func New(baseTime int64) *Day {
	if baseTime < 1e10 {
		panic("invalid base time, use decisecond")
	}
	d := &Day{baseTime: baseTime / day10 * day10}
	for i := range d.hours {
		d.hours[i] = newHourMap(d.baseTime + hour10*int64(i))
	}
	return d
}

func (b *Day) BaseTime() int64 {
	return b.baseTime
}

type hourMap struct {
	mu       sync.RWMutex
	baseTime int64
	fwd      *roaring.Bitmap // 16 bits ts + 16 bits hash
	back     *roaring.Bitmap // 16 bits hash idx + 16 bits ts
	hashCtr  uint64
	hashIdx  map[uint32]uint16
}

func newHourMap(baseTime int64) *hourMap {
	m := &hourMap{
		baseTime: baseTime / hour10 * hour10,
		fwd:      roaring.New(),
		back:     roaring.New(),
		hashIdx:  map[uint32]uint16{},
	}
	return m
}

func (b *Day) Add(ts int64, v uint32) bool {
	return b.hours[(ts-b.baseTime)/hour10].add(ts, v)
}

func (b *hourMap) add(ts int64, v uint32) bool {
	if ts-b.baseTime > hour10 || ts < b.baseTime {
		panic(fmt.Sprintf("invalid timestamp: %v, base: %v", ts, b.baseTime))
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	offset := uint32(ts - b.baseTime)
	if idx, ok := b.hashIdx[v]; ok {
		return b.back.CheckedAdd(uint32(idx)<<16 | offset)
	}

	h := h16(offset, v)
	if !b.fwd.Contains(h) {
		if b.fwd.AndCardinality(andTable[offset]) > 1024 {
			// Bitmap is too dense.
			idx := uint16(b.hashCtr & 0xffff)
			b.hashIdx[v] = idx
			b.hashCtr++
			return b.back.CheckedAdd(uint32(idx)<<16 | offset)
		}
	}
	return b.fwd.CheckedAdd(h)
}

func (b *Day) Join(vs []uint32) (res JoinedResult) {
	for i := 23; i >= 0; i-- {
		b.hours[i].join(vs, i, &res)
	}
	return
}

func (b *hourMap) join(vs []uint32, hr int, res *JoinedResult) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	res.hours[hr].m = roaring.New()
	res.hours[hr].baseTime = b.baseTime
	res.hours[hr].scores = map[uint32]int8{}

	iter := b.fwd.Iterator()
	for iter.HasNext() {
		offset := iter.Next() >> 16

		for _, v := range vs {
			if b.fwd.Contains(h16(offset, v)) {
				res.hours[hr].m.Add(offset)
				res.hours[hr].scores[offset]++
			}
		}

		iter.AdvanceIfNeeded((offset + 1) << 16)
	}

	for _, v := range vs {
		if idx, ok := b.hashIdx[v]; ok {
			iter := b.back.Iterator()
			iter.AdvanceIfNeeded(uint32(idx) << 16)
			for iter.HasNext() {
				v := iter.Next()
				if v>>16 != uint32(idx) {
					break
				}
				offset := v & 0xffff

				res.hours[hr].m.Add(offset)
				res.hours[hr].scores[offset]++
			}
		}
	}
}

func UnmarshalBinary(p []byte) (*Day, error) {
	rd := bytes.NewReader(p)

	var ver byte
	if err := binary.Read(rd, binary.BigEndian, &ver); err != nil {
		return nil, fmt.Errorf("read version: %v", err)
	}

	b := &Day{}
	if err := binary.Read(rd, binary.BigEndian, &b.baseTime); err != nil {
		return nil, fmt.Errorf("read baseTime: %v", err)
	}

	for i := range b.hours {
		var err error
		b.hours[i], err = readHourMap(rd)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func readHourMap(rd io.Reader) (*hourMap, error) {
	// var ver byte
	// if err := binary.Read(rd, binary.BigEndian, &ver); err != nil {
	// 	return nil, fmt.Errorf("read version: %v", err)
	// }

	b := &hourMap{}
	if err := binary.Read(rd, binary.BigEndian, &b.baseTime); err != nil {
		return nil, fmt.Errorf("read baseTime: %v", err)
	}

	var topSize uint64
	if err := binary.Read(rd, binary.BigEndian, &topSize); err != nil {
		return nil, fmt.Errorf("read fwd bitmap size: %v", err)
	}

	b.fwd = roaring.New()
	if _, err := b.fwd.ReadFrom(io.LimitReader(rd, int64(topSize))); err != nil {
		return nil, fmt.Errorf("read fwd bitmap: %v", err)
	}

	if err := binary.Read(rd, binary.BigEndian, &topSize); err != nil {
		return nil, fmt.Errorf("read back bitmap size: %v", err)
	}

	b.back = roaring.New()
	if _, err := b.back.ReadFrom(io.LimitReader(rd, int64(topSize))); err != nil {
		return nil, fmt.Errorf("read back bitmap: %v", err)
	}

	if err := binary.Read(rd, binary.BigEndian, &b.hashCtr); err != nil {
		return nil, fmt.Errorf("read hashCtr: %v", err)
	}

	if err := binary.Read(rd, binary.BigEndian, &topSize); err != nil {
		return nil, fmt.Errorf("read hashIdx size: %v", err)
	}

	b.hashIdx = make(map[uint32]uint16, topSize)
	for i := 0; i < int(topSize); i++ {
		var k uint32
		var v uint16
		if err := binary.Read(rd, binary.BigEndian, &k); err != nil {
			return nil, fmt.Errorf("read hashIdx key: %v", err)
		}
		if err := binary.Read(rd, binary.BigEndian, &v); err != nil {
			return nil, fmt.Errorf("read hashIdx value: %v", err)
		}
		b.hashIdx[k] = v
	}
	return b, nil
}

func (b *Day) MarshalBinary() []byte {
	p := &bytes.Buffer{}
	p.WriteByte(1)
	binary.Write(p, binary.BigEndian, b.baseTime)
	for _, h := range b.hours {
		h.writeTo(p)
	}
	return p.Bytes()
}

func (b *hourMap) writeTo(buf io.Writer) {
	b.mu.Lock()
	defer b.mu.Unlock()

	binary.Write(buf, binary.BigEndian, b.baseTime)

	binary.Write(buf, binary.BigEndian, b.fwd.GetSerializedSizeInBytes())
	b.fwd.WriteTo(buf)

	binary.Write(buf, binary.BigEndian, b.back.GetSerializedSizeInBytes())
	b.back.WriteTo(buf)

	binary.Write(buf, binary.BigEndian, b.hashCtr)
	binary.Write(buf, binary.BigEndian, uint64(len(b.hashIdx)))
	for k, v := range b.hashIdx {
		binary.Write(buf, binary.BigEndian, k)
		binary.Write(buf, binary.BigEndian, v)
	}
}

func h16(offset, v uint32) uint32 {
	v = combinehash(v, offset) & 0xffff
	return offset<<16 | v
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

func (b *Day) String() string {
	buf := &bytes.Buffer{}
	for _, h := range b.hours {
		h.debug(buf)
		buf.WriteByte('\n')
	}
	return buf.String()
}

func (b *hourMap) debug(buf io.Writer) {
	fmt.Fprintf(buf, "[%v] ", time.Unix(b.baseTime/10, 0).Format(time.ANSIC))
	fmt.Fprintf(buf, "fwd: %d (size=%db) ", b.fwd.GetCardinality(), b.fwd.GetSerializedSizeInBytes())
	fmt.Fprintf(buf, "back: %d (size=%db) ", b.back.GetCardinality(), b.back.GetSerializedSizeInBytes())
	fmt.Fprintf(buf, "hash: %d (loop=%v, ctr=%x)", len(b.hashIdx), b.hashCtr/0xffff, b.hashCtr)

	// var tmp [64]int
	// for _, v := range b.hashIdx {
	// 	tmp[v/1024]++
	// }

	// for i := 0; i < len(tmp); i += 4 {
	// 	for j := 0; j < 4; j++ {
	// 		x := (i + j)
	// 		fmt.Fprintf(buf, "%04x-%04x %-5d\t", x*1024, (x+1)*1024-1, tmp[x])
	// 	}
	// 	buf.WriteString("\n")
	// }
}

type JoinedResult struct {
	hours [24]struct {
		m        *roaring.Bitmap
		scores   map[uint32]int8
		baseTime int64
	}
}

func (br JoinedResult) Iterate(f func(ts int64, scores int) bool) {
	for i := 23; i >= 0; i-- {
		iter := br.hours[i].m.ReverseIterator()
		for iter.HasNext() {
			v := iter.Next()
			if !f(br.hours[i].baseTime+int64(v), int(br.hours[i].scores[v])) {
				break
			}
		}
	}
}
