package bitmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/coyove/sdss/contrib/clock"
)

const (
	hour10 = 3600 * 10
	day    = 86400
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

func New(baseTime int64, hashNum int8) *Day {
	d := &Day{baseTime: baseTime / day * day * 10}
	for i := range d.hours {
		d.hours[i] = newHourMap(d.baseTime+hour10*int64(i), hashNum)
	}
	return d
}

func (b *Day) BaseTime() int64 {
	return b.baseTime / 10
}

type hourMap struct {
	mu       sync.RWMutex
	baseTime int64 // baseTime + 16 bits ts = final timestamp
	hashNum  int8

	fwd  *roaring.Bitmap // 16 bits ts + 16 bits hash
	back *roaring.Bitmap // 16 bits hash idx + 16 bits ts

	hashCtr uint64
	hashIdx map[uint32]uint16

	keys []uint64 // keys
	maps []uint16 // key -> ts offset maps
}

func newHourMap(baseTime int64, hashNum int8) *hourMap {
	m := &hourMap{
		baseTime: baseTime / hour10 * hour10,
		hashNum:  hashNum,
		fwd:      roaring.New(),
		back:     roaring.New(),
		hashIdx:  map[uint32]uint16{},
	}
	return m
}

func (b *Day) Add(key uint64, v []uint32) {
	b.addWithTime(key, clock.UnixDeci(), v)
}

func (b *Day) addWithTime(key uint64, ts int64, v []uint32) {
	b.hours[(ts-b.baseTime)/hour10].add(ts, key, v)
}

func (b *hourMap) add(ts int64, key uint64, vs []uint32) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ts-b.baseTime > hour10 || ts < b.baseTime {
		panic(fmt.Sprintf("invalid timestamp: %v, base: %v", ts, b.baseTime))
	}

	offset := uint32(ts - b.baseTime)

	if len(b.maps) > 0 {
		// if key <= b.keys[len(b.keys)-1] {
		// 	panic(fmt.Sprintf("invalid key: %016x, last key: %016x", key, b.keys[len(b.keys)-1]))
		// }
		if uint16(offset) < b.maps[len(b.maps)-1] {
			panic(fmt.Sprintf("invalid timestamp: %d, last: %d", offset, b.maps[len(b.keys)-1]))
		}
	}

	b.keys = append(b.keys, key)
	b.maps = append(b.maps, uint16(offset))

	for _, v := range vs {
		if idx, ok := b.hashIdx[v]; ok {
			b.back.Add(uint32(idx)<<16 | offset)
			continue
		}

		if b.fwd.AndCardinality(andTable[offset]) > 1024*uint64(b.hashNum) {
			// Bitmap is too dense.
			idx := uint16(b.hashCtr & 0xffff)
			b.hashIdx[v] = idx
			b.hashCtr++
			b.back.Add(uint32(idx)<<16 | offset)
			continue
		}

		h := h16(offset, v)
		for i := 0; i < int(b.hashNum); i++ {
			b.fwd.Add(h[i])
		}
	}
}

func (b *Day) Join(vs []uint32, n int, desc bool) (res []KeyTimeScore) {
	for i := 23; i >= 0; i-- {
		b.hours[i].join(vs, i, &res)
		if n > 0 && len(res) >= n {
			break
		}
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].Time == res[j].Time {
			if desc {
				return res[i].Key > res[j].Key
			}
			return res[i].Key < res[j].Key
		}
		if desc {
			return res[i].Time > res[j].Time
		}
		return res[i].Time < res[j].Time
	})
	return
}

func (b *hourMap) join(vs []uint32, hr int, res *[]KeyTimeScore) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	m := roaring.New()
	scores := map[uint32]int{}

	iter := b.fwd.Iterator()
	for iter.HasNext() {
		offset := iter.Next() >> 16

		for _, v := range vs {
			h, s := h16(offset, v), 0
			for i := 0; i < int(b.hashNum); i++ {
				if b.fwd.Contains(h[i]) {
					s++
				}
			}
			if s == int(b.hashNum) {
				m.Add(offset)
				scores[offset]++
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
				m.Add(offset)
				scores[offset]++
			}
		}
	}

	for iter, i := m.Iterator(), 0; iter.HasNext(); {
		offset := uint16(iter.Next())
		for ; i < len(b.keys); i++ {
			if b.maps[i] < offset {
				continue
			}
			if b.maps[i] > offset {
				break
			}
			*res = append(*res, KeyTimeScore{
				Key:   b.keys[i],
				Time:  (b.baseTime + int64(b.maps[i])) / 10,
				Score: int(scores[uint32(offset)]),
			})
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
	b := &hourMap{}
	if err := binary.Read(rd, binary.BigEndian, &b.baseTime); err != nil {
		return nil, fmt.Errorf("read baseTime: %v", err)
	}

	if err := binary.Read(rd, binary.BigEndian, &b.hashNum); err != nil {
		return nil, fmt.Errorf("read hashNum: %v", err)
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

	var keysLen uint32
	if err := binary.Read(rd, binary.BigEndian, &keysLen); err != nil {
		return nil, fmt.Errorf("read keys length: %v", err)
	}

	b.keys = make([]uint64, keysLen)
	if err := binary.Read(rd, binary.BigEndian, b.keys); err != nil {
		return nil, fmt.Errorf("read keys: %v", err)
	}

	b.maps = make([]uint16, keysLen)
	if err := binary.Read(rd, binary.BigEndian, b.maps); err != nil {
		return nil, fmt.Errorf("read maps: %v", err)
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
	binary.Write(buf, binary.BigEndian, b.hashNum)

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

	binary.Write(buf, binary.BigEndian, uint32(len(b.keys)))
	binary.Write(buf, binary.BigEndian, b.keys)
	binary.Write(buf, binary.BigEndian, b.maps)
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
	fmt.Fprintf(buf, "[%v] #hash: %d, ", time.Unix(b.baseTime/10, 0).Format("06-01-02-15"), b.hashNum)
	if len(b.keys) > 0 {
		fmt.Fprintf(buf, "keys: %d (last=%016x-->%d), ",
			len(b.keys), b.keys[len(b.keys)-1], b.maps[len(b.maps)-1])
	} else {
		fmt.Fprintf(buf, "keys: none, ")
	}
	fmt.Fprintf(buf, "fwd: %d (size=%db), ", b.fwd.GetCardinality(), b.fwd.GetSerializedSizeInBytes())
	fmt.Fprintf(buf, "back: %d (size=%db), ", b.back.GetCardinality(), b.back.GetSerializedSizeInBytes())
	fmt.Fprintf(buf, "raw: %d (loop=%v, ctr=%x)", len(b.hashIdx), b.hashCtr/0xffff, b.hashCtr)
}
