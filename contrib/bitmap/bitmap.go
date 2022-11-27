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

	table *roaring.Bitmap // 20 bits hash + 12 bits ts
	keys  []uint64        // keys
	maps  []uint16        // key -> ts offset maps
}

func newHourMap(baseTime int64, hashNum int8) *hourMap {
	m := &hourMap{
		baseTime: baseTime / hour10 * hour10,
		hashNum:  hashNum,
		table:    roaring.New(),
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
		h := h16(v, b.baseTime)
		for i := 0; i < int(b.hashNum); i++ {
			b.table.Add(h[i] + offset)
		}
	}
}

func (b *Day) Join(hashes []uint32, n int) (res []KeyTimeScore) {
	for i := 23; i >= 0; i-- {
		b.hours[i].join(hashes, i, &res)
		if n > 0 && len(res) >= n {
			break
		}
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].UnixDeci == res[j].UnixDeci {
			return res[i].Key > res[j].Key
		}
		return res[i].UnixDeci > res[j].UnixDeci
	})
	return
}

func (b *hourMap) join(vs []uint32, hr int, res *[]KeyTimeScore) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	type hashState struct {
		d int
		v uint32
		roaring.Bitmap
	}

	hashes := map[uint32]*hashState{}
	hashSort := []uint32{}
	for _, v := range vs {
		h := h16(v, b.baseTime, d)
		s := &hashState{
			d: d,
			v: v,
		}
		for i := 0; i < int(b.hashNum); i++ {
			hashes[h[i]] = s
			hashSort = append(hashSort, h[i])
		}
	}
	sort.Slice(hashSort, func(i, j int) bool { return hashSort[i] < hashSort[j] })

	scores := map[uint32]int{}
	iter := b.table.Iterator()
	for _, h := range hashSort {
		iter.AdvanceIfNeeded(h << 12)
		for iter.HasNext() {
			v := iter.Next()
			if v>>12 != h {
				break
			}
			ts := (v&0xfff)*10 + uint32(hashes[h].d)
			hashes[h].Add(ts)
			scores[ts]++
		}
	}

	var final *roaring.Bitmap
	for _, h := range hashes {
		if final == nil {
			final = &h.Bitmap
		} else {
			final.Or(&h.Bitmap)
		}
	}
	fmt.Println(final)

	for iter, i := final.Iterator(), 0; iter.HasNext(); {
		offset := uint16(iter.Next())
		for ; i < len(b.keys); i++ {
			if b.maps[i] < offset {
				continue
			}
			if b.maps[i] > offset {
				break
			}
			*res = append(*res, KeyTimeScore{
				Key:      b.keys[i],
				UnixDeci: (b.baseTime + int64(b.maps[i])),
				Score:    int(scores[uint32(offset)]),
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
		return nil, fmt.Errorf("read table bitmap size: %v", err)
	}

	b.table = roaring.New()
	if _, err := b.table.ReadFrom(io.LimitReader(rd, int64(topSize))); err != nil {
		return nil, fmt.Errorf("read table bitmap: %v", err)
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

	binary.Write(buf, binary.BigEndian, b.table.GetSerializedSizeInBytes())
	b.table.WriteTo(buf)

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
	fmt.Fprintf(buf, "[%d;%v] #hash: %d, ",
		b.baseTime/10, time.Unix(b.baseTime/10, 0).Format("01-02;15"), b.hashNum)
	if len(b.keys) > 0 {
		fmt.Fprintf(buf, "keys: %d (last=%016x-->%d), ",
			len(b.keys), b.keys[len(b.keys)-1], b.maps[len(b.maps)-1])
	} else {
		fmt.Fprintf(buf, "keys: none, ")
	}
	fmt.Fprintf(buf, "table: %d (size=%db)", b.table.GetCardinality(), b.table.GetSerializedSizeInBytes())
}
