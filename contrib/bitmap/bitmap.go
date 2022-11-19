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
	halfday          = 43200
	span             = 600 // 10 minutes range
	lowhighThreshold = 4096
)

var andTable [halfday / span]*roaring.Bitmap

func init() {
	for i := range andTable {
		andTable[i] = roaring.New()
		andTable[i].AddRange(uint64(i*span)<<16, uint64(i*span+span)<<16)
	}
	return
}

type HalfDay struct {
	mu          sync.RWMutex
	baseTime    int64
	lastCompact int16

	low  *roaring.Bitmap   // 16 bits ts + 16 bits hash
	high *roaring64.Bitmap // 16 bits ts + 24 bits hash

	tsq [halfday / 8]byte
}

func New(baseTime int64) *HalfDay {
	m := &HalfDay{
		baseTime: baseTime / halfday * halfday,
		low:      roaring.New(),
		high:     roaring64.New(),
	}
	return m
}

func (b *HalfDay) BaseTime() int64 {
	return b.baseTime
}

func (b *HalfDay) Add(ts int64, v uint32) bool {
	if ts-b.baseTime > halfday || ts < b.baseTime {
		panic(fmt.Sprintf("invalid timestamp: %v, base: %v", ts, b.baseTime))
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	offset := uint32(ts - b.baseTime)
	if int16(offset/span) < b.lastCompact {
		panic(fmt.Sprintf("access to compacted area: %v, last compact: %v", offset/span, b.lastCompact))
	}

	b.low.Add(h16(offset, v))
	added := b.high.CheckedAdd(h24(offset, v))
	if added {
		b.tsq[offset/8] |= 1 << (offset % 8)
	}
	return added
}

func (b *HalfDay) Merge(vs []uint32) (res MergeResult) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	res.m = roaring.New()
	res.baseTime = b.baseTime

	hyped := false
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
			hyped = true
		}
	}

	if hyped {
		return
	}

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
	return
}

func UnmarshalBinary(buf []byte) (*HalfDay, error) {
	//rd, err := gzip.NewReader(bytes.NewReader(buf))
	//if err != nil {
	//	return nil, fmt.Errorf("create gzip reader: %v", err)
	//}
	rd := bytes.NewReader(buf)

	b := &HalfDay{}
	if err := binary.Read(rd, binary.BigEndian, &b.baseTime); err != nil {
		return nil, fmt.Errorf("read baseTime: %v", err)
	}

	if err := binary.Read(rd, binary.BigEndian, &b.lastCompact); err != nil {
		return nil, fmt.Errorf("read lastCompact: %v", err)
	}

	if err := binary.Read(rd, binary.BigEndian, b.tsq[:]); err != nil {
		return nil, fmt.Errorf("read tsq: %v", err)
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

func (b *HalfDay) MarshalBinary(compactBefore int64) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	hend := int((compactBefore - b.baseTime) / span)
	if hend > len(andTable) {
		hend = len(andTable)
	}
	for h := b.lastCompact; h < int16(hend); h++ {
		sec := uint64(h) * span
		count := b.low.AndCardinality(andTable[h])
		dist := b.getTimeDistInSpan(int(sec))
		if int(count) > dist*lowhighThreshold {
			// "low" area is dense, use "high" area.
			b.low.RemoveRange(sec<<16, (sec+span)<<16)
			// fmt.Println(h, "high", count)
		} else {
			// "low" area is sparse, no need to store "high" area.
			b.high.RemoveRange(sec<<24, (sec+span)<<24)
			// fmt.Println(h, "low", count)
		}
	}

	b.lastCompact = int16(hend)

	out := &bytes.Buffer{}
	// buf := gzip.NewWriter(out)
	buf := out

	binary.Write(buf, binary.BigEndian, b.baseTime)
	binary.Write(buf, binary.BigEndian, b.lastCompact)
	binary.Write(buf, binary.BigEndian, b.tsq[:])

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

func (b *HalfDay) getTimeDistInSpan(spanStart int) (res int) {
	for ts := spanStart; ts < spanStart+span; ts++ {
		if (b.tsq[ts/8]>>(ts%8))&1 == 1 {
			res++
		}
	}
	return res
}

func (b *HalfDay) String() string {
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "base:    %v\n", time.Unix(b.baseTime, 0).Format(time.ANSIC))
	fmt.Fprintf(buf, "compact: %v\n", b.lastCompact)
	fmt.Fprintf(buf, "low:     %v\n", b.low.GetSerializedSizeInBytes())
	fmt.Fprintf(buf, "high:    %v\n", b.high.GetSerializedSizeInBytes())
	fmt.Fprintf(buf, "#low:    %v\n", b.low.GetCardinality())
	fmt.Fprintf(buf, "#high:   %v\n", b.high.GetCardinality())

	const N = 3
	for i := 0; i < N; i++ {
		fmt.Fprintf(buf, "     %-4s %-10s %-10s", "dist", "low", "high")
	}
	fmt.Fprintf(buf, "\n")

	for h := 0; h < len(andTable); h += 3 {
		m := [N]*roaring64.Bitmap{}
		for i := range m {
			m[i] = roaring64.New()
			m[i].AddRange(uint64((h+i)*span)<<24, uint64((h+i)*span+span)<<24)
		}
		for i := 0; i < N; i++ {
			fmt.Fprintf(buf, "[%02d] %-4v %-10v %-10v",
				h+i, b.getTimeDistInSpan((h+i)*span), b.low.AndCardinality(andTable[h+i]), b.high.AndCardinality(m[i]),
			)
		}
		fmt.Fprintf(buf, "\n")
	}
	return buf.String()
}
