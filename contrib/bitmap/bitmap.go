package bitmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/RoaringBitmap/roaring/roaring64"
)

const (
	halfday = 43200
	span    = 600 // 10 minutes range
	// 1800 271
	// 1200 266
	// 600 263
)

var andTable [halfday / span]*roaring.Bitmap

func init() {
	for i := range andTable {
		andTable[i] = roaring.New()
		andTable[i].AddRange(uint64(i*span)<<16, uint64(i*span+span)<<16)
	}
	return
}

type Bitmap struct {
	mu          sync.RWMutex
	baseTime    int64
	lastCompact int16

	low  *roaring.Bitmap   // 16 bits ts + 16 bits hash
	high *roaring64.Bitmap // 16 bits ts + 24 bits hash
}

func New(baseTime int64) *Bitmap {
	m := &Bitmap{
		baseTime: baseTime / halfday * halfday,
		low:      roaring.New(),
		high:     roaring64.New(),
	}
	return m
}

func (b *Bitmap) Add(ts int64, v uint32) bool {
	if ts-b.baseTime > halfday {
		panic(fmt.Sprintf("invalid timestamp: %v, base: %v", ts, b.baseTime))
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	offset := uint32(ts - b.baseTime)
	if int16(offset/span) < b.lastCompact {
		panic(fmt.Sprintf("access to compacted area: %v, last compact: %v", offset/span, b.lastCompact))
	}

	b.low.Add(h16(offset, v))
	return b.high.CheckedAdd(h24(offset, v))
}

func (b *Bitmap) Merge(vs []uint32) (res MergeResult) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	res.m = roaring.New()
	res.baseTime = b.baseTime

	{ // iterate "low" area
		iter := b.low.Iterator()
		for iter.HasNext() {
			offset := iter.Next() >> 16

			for _, v := range vs {
				if b.low.Contains(h16(offset, v)) {
					res.m.Add(uint32(offset))
					break
				}
			}

			iter.AdvanceIfNeeded((offset + 1) << 16)
		}
	}

	{ // iterate "high" area
		iter := b.high.Iterator()
		for iter.HasNext() {
			offset := iter.Next() >> 24

			for _, v := range vs {
				if b.high.Contains(h24(uint32(offset), v)) {
					res.m.Add(uint32(offset))
					break
				}
			}

			iter.AdvanceIfNeeded((offset + 1) << 24)
		}
	}
	return
}

func UnmarshalBinary(buf []byte) (*Bitmap, error) {
	//rd, err := gzip.NewReader(bytes.NewReader(buf))
	//if err != nil {
	//	return nil, fmt.Errorf("create gzip reader: %v", err)
	//}
	rd := bytes.NewReader(buf)

	b := &Bitmap{}
	if err := binary.Read(rd, binary.BigEndian, &b.baseTime); err != nil {
		return nil, fmt.Errorf("read baseTime: %v", err)
	}

	if err := binary.Read(rd, binary.BigEndian, &b.lastCompact); err != nil {
		return nil, fmt.Errorf("read lastCompact: %v", err)
	}

	var topSize uint64
	if err := binary.Read(rd, binary.BigEndian, &topSize); err != nil {
		return nil, fmt.Errorf("read low bitmap size: %v", err)
	}

	b.low = roaring.New()
	if _, err := b.low.ReadFrom(io.LimitReader(rd, int64(topSize))); err != nil {
		return nil, fmt.Errorf("read low bitmap: %v", err)
	}

	if err := binary.Read(rd, binary.BigEndian, &topSize); err != nil {
		return nil, fmt.Errorf("read high bitmap size: %v", err)
	}

	b.high = roaring64.New()
	if _, err := b.high.ReadFrom(io.LimitReader(rd, int64(topSize))); err != nil {
		return nil, fmt.Errorf("read high bitmap: %v", err)
	}

	return b, nil
}

func (b *Bitmap) MarshalBinary(compactBefore int64) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	hend := int((compactBefore - b.baseTime) / span)
	if hend > len(andTable) {
		hend = len(andTable)
	}
	for h := b.lastCompact; h < int16(hend); h++ {
		sec := uint64(h) * span
		count := b.low.AndCardinality(andTable[h])
		if count > span*4096 {
			// "low" area is dense, use "high" area.
			b.low.RemoveRange(sec<<16, (sec+span)<<16)
			fmt.Println(h, "high", count)
		} else {
			// "low" area is sparse, no need to store "high" area.
			b.high.RemoveRange(sec<<24, (sec+span)<<24)
			fmt.Println(h, "low", count)
		}
	}

	b.lastCompact = int16(hend)

	out := &bytes.Buffer{}
	// buf := gzip.NewWriter(out)
	buf := out

	binary.Write(buf, binary.BigEndian, b.baseTime)
	binary.Write(buf, binary.BigEndian, b.lastCompact)

	binary.Write(buf, binary.BigEndian, b.low.GetSerializedSizeInBytes())
	b.low.WriteTo(buf)

	binary.Write(buf, binary.BigEndian, b.high.GetSerializedSizeInBytes())
	b.high.WriteTo(buf)

	// buf.Close()

	return out.Bytes()
}

type MergeResult struct {
	m        *roaring.Bitmap
	baseTime int64
}

func (br MergeResult) Iterate(f func(ts int64) bool) {
	iter := br.m.ReverseIterator()
	for iter.HasNext() {
		if !f(br.baseTime + int64(iter.Next())) {
			break
		}
	}
}

func h16(offset, v uint32) uint32 {
	v = combinehash(v, offset) & 0xffff
	if v == 0 {
		v++
	}
	return offset<<16 | v
}

func h24(offset, v uint32) uint64 {
	v = combinehash(v, offset) & 0xffffff
	if v == 0 {
		v++
	}
	return uint64(offset)<<24 | uint64(v)
}

func combinehash(a, b uint32) uint32 {
	a *= 16777619
	a ^= b
	return a
}

func (b *Bitmap) String() string {
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "base:    %v\n", time.Unix(b.baseTime, 0).Format(time.ANSIC))
	fmt.Fprintf(buf, "compact: %v\n", b.lastCompact)
	fmt.Fprintf(buf, "low:     %v\n", b.low.GetSerializedSizeInBytes())
	fmt.Fprintf(buf, "high:    %v\n", b.high.GetSerializedSizeInBytes())
	fmt.Fprintf(buf, "#low:    %v\n", b.low.GetCardinality())
	fmt.Fprintf(buf, "#high:   %v\n", b.high.GetCardinality())
	fmt.Fprintf(buf, "     %-10s %-10s      %-10s %-10s      %-10s %-10s\n", "low", "high", "low", "high", "low", "high")

	for h := 0; h < len(andTable); h += 3 {
		m := [3]*roaring64.Bitmap{}
		for i := range m {
			m[i] = roaring64.New()
			m[i].AddRange(uint64((h+i)*span)<<24, uint64((h+i)*span+span)<<24)
		}
		fmt.Fprintf(buf, "[%02d] %-10v %-10v [%02d] %-10v %-10v [%02d] %-10v %-10v\n",
			h+0, b.low.AndCardinality(andTable[h]), b.high.AndCardinality(m[0]),
			h+1, b.low.AndCardinality(andTable[h+1]), b.high.AndCardinality(m[1]),
			h+2, b.low.AndCardinality(andTable[h+2]), b.high.AndCardinality(m[2]),
		)
	}
	return buf.String()
}
