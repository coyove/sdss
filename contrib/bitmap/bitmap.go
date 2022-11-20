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
	"github.com/RoaringBitmap/roaring/roaring64"
)

const (
	halfday          = 43200
	span             = 600 // 10 minutes range
	lowhighThreshold = 4096
	highbits         = 28
	tagbits          = 4
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
	high *roaring64.Bitmap // 16 bits ts + 4 bits tag + 24 bits hash
	tsq  *roaring.Bitmap   // bitmap of time offset [0, 43200), Add(ts, ...) sets the corresponding bit
}

func New(baseTime int64) *HalfDay {
	m := &HalfDay{
		baseTime: baseTime / halfday * halfday,
		low:      roaring.New(),
		high:     roaring64.New(),
		tsq:      roaring.New(),
	}
	return m
}

func (b *HalfDay) BaseTime() int64 {
	return b.baseTime
}

func (b *HalfDay) Add(ts int64, tag uint8, v uint32) bool {
	if ts-b.baseTime > halfday || ts < b.baseTime {
		panic(fmt.Sprintf("invalid timestamp: %v, base: %v", ts, b.baseTime))
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	offset := uint32(ts - b.baseTime)
	if int16(offset/span) < b.lastCompact {
		panic(fmt.Sprintf("access to compacted area: %v, last compact: %v", offset/span, b.lastCompact))
	}

	b.tsq.Add(offset)
	b.low.Add(h16(offset, v))
	return b.high.CheckedAdd(h24(offset, tag, v))
}

func (b *HalfDay) Join(vs []uint32) (res JoinedResult) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	res.m = roaring.New()
	res.baseTime = b.baseTime
	res.scores = map[uint32]int8{}

	dedup := roaring.New()
	{ // iterate "high" area
		iter := b.high.Iterator()
		for iter.HasNext() {
			offset := iter.Next() >> highbits
			dedup.Add(uint32(offset))

			for _, v := range vs {
				for tag := 0; tag < 1<<tagbits; tag++ {
					if b.high.Contains(h24(uint32(offset), uint8(tag), v)) {
						x := uint32(offset)<<8 | uint32(tag)
						res.m.Add(x)
						res.scores[x]++
					}
				}
			}

			iter.AdvanceIfNeeded((offset + 1) << highbits)
		}
	}

	{ // iterate "low" area
		iter := b.low.Iterator()
		for iter.HasNext() {
			offset := iter.Next() >> 16
			if dedup.Contains(offset) {
				continue
			}

			for _, v := range vs {
				if b.low.Contains(h16(offset, v)) {
					res.m.Add(uint32(offset))
					res.scores[uint32(offset)]++
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

	var ver byte
	if err := binary.Read(rd, binary.BigEndian, &ver); err != nil {
		return nil, fmt.Errorf("read version: %v", err)
	}

	b := &HalfDay{}
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

	if err := binary.Read(rd, binary.BigEndian, &topSize); err != nil {
		return nil, fmt.Errorf("read tsq bitmap size: %v", err)
	}

	b.tsq = roaring.New()
	if _, err := b.tsq.ReadFrom(io.LimitReader(rd, int64(topSize))); err != nil {
		return nil, fmt.Errorf("read tsq bitmap: %v", err)
	}

	return b, nil
}

func (b *HalfDay) MarshalBinary(compactBefore int64) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	hend := int((compactBefore - b.baseTime) / span)
	if hend > len(andTable) {
		hend = len(andTable)
	} else if hend < 0 {
		hend = 0
	}
	for h := b.lastCompact; h < int16(hend); h++ {
		sec := uint64(h) * span
		count := b.low.AndCardinality(andTable[h])
		dist := b.getTimeDistInSpan(int(sec))
		if int(count) > dist*lowhighThreshold {
			// "low" area is dense, use "high" area.
			b.low.RemoveRange(sec<<16, (sec+span)<<16)
		} else {
			// "low" area is sparse, no need to store "high" area.
			b.high.RemoveRange(sec<<highbits, (sec+span)<<highbits)
		}
	}

	b.lastCompact = int16(hend)

	out := &bytes.Buffer{}
	// buf := gzip.NewWriter(out)
	buf := out

	binary.Write(buf, binary.BigEndian, byte(1))
	binary.Write(buf, binary.BigEndian, b.baseTime)
	binary.Write(buf, binary.BigEndian, b.lastCompact)

	binary.Write(buf, binary.BigEndian, b.low.GetSerializedSizeInBytes())
	b.low.WriteTo(buf)

	binary.Write(buf, binary.BigEndian, b.high.GetSerializedSizeInBytes())
	b.high.WriteTo(buf)

	binary.Write(buf, binary.BigEndian, b.tsq.GetSerializedSizeInBytes())
	b.tsq.WriteTo(buf)

	// buf.Close()

	return out.Bytes()
}

func h16(offset, v uint32) uint32 {
	v = combinehash(v, offset) & 0xffff
	return offset<<16 | v
}

func h24(offset uint32, tag uint8, v uint32) uint64 {
	v = combinehash(v, offset) & 0xffffff
	return uint64(offset)<<highbits | uint64(tag&(1<<tagbits-1))<<24 | uint64(v)
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

func (b *HalfDay) getTimeDistInSpan(spanStart int) (res int) {
	for ts := spanStart; ts < spanStart+span; ts++ {
		if b.tsq.ContainsInt(ts) {
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
		for i := 0; i < N; i++ {
			fmt.Fprintf(buf, "[%02d] %-4v %-10v %-10v",
				h+i,
				b.getTimeDistInSpan((h+i)*span),
				b.low.AndCardinality(andTable[h+i]),
				b.high.Rank(uint64((h+i)*span+span)<<highbits-1)-b.high.Rank(uint64((h+i)*span)<<highbits-1),
			)
		}
		fmt.Fprintf(buf, "\n")
	}
	return buf.String()
}

type JoinedResult struct {
	m        *roaring.Bitmap
	scores   map[uint32]int8
	baseTime int64
}

func (br JoinedResult) Iterate(f func(ts int64, tag, scores int) bool) {
	iter := br.m.ReverseIterator()
	for iter.HasNext() {
		v := iter.Next()
		if v > halfday {
			if !f(br.baseTime+int64(v>>8), int(v&0xff), int(br.scores[v])) {
				break
			}
		} else {
			if !f(br.baseTime+int64(v), -1, int(br.scores[v])) {
				break
			}
		}
	}
}
