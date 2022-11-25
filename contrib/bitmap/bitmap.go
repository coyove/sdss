package bitmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
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

type Day struct {
	mfmu     sync.Mutex
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
	baseTime int64 // baseTime + 12 bits ts = final timestamp
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
		h := h16(v, b.baseTime, int(offset%10))
		for i := 0; i < int(b.hashNum); i++ {
			b.table.Add(h[i]*3600 + (offset / 10))
		}
	}
}

func (b *Day) Join(hashes []uint32, count int, joinType int) (res []KeyTimeScore) {
	for i := 23; i >= 0; i-- {
		b.hours[i].join(hashes, i, joinType, &res)
		if count > 0 && len(res) >= count {
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

func (b *hourMap) join(vs []uint32, hr int, joinType int, res *[]KeyTimeScore) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	type hashState struct {
		d int
		roaring.Bitmap
	}

	hashes := map[uint32]*hashState{}
	hashSort := []uint32{}
	vsHashes := [][4]uint32{}
	for _, v := range vs {
		for d := 0; d < 10; d++ {
			h := h16(v, b.baseTime, d)
			for i := 0; i < int(b.hashNum); i++ {
				hashes[h[i]] = &hashState{d: d}
				hashSort = append(hashSort, h[i])
			}
			vsHashes = append(vsHashes, h)
		}
	}
	sort.Slice(hashSort, func(i, j int) bool { return hashSort[i] < hashSort[j] })

	iter := b.table.Iterator()
	for _, h := range hashSort {
		iter.AdvanceIfNeeded(h * 3600)
		for iter.HasNext() {
			v := iter.Next()
			if v/3600 != h {
				break
			}
			ts := (v%3600)*10 + uint32(hashes[h].d)
			hashes[h].Add(ts)
		}
	}

	scores := map[uint32]int{}
	final := roaring.New()
	for i := range vsHashes {
		raw := vsHashes[i]
		m := &hashes[raw[0]].Bitmap
		for i := 1; i < int(b.hashNum); i++ {
			m.And(&hashes[raw[i]].Bitmap)
		}
		m.Iterate(func(x uint32) bool { scores[x]++; return true })
		final.Or(m)
	}

	for iter, i := final.Iterator(), 0; iter.HasNext(); {
		offset := uint16(iter.Next())
		for ; i < len(b.keys); i++ {
			if b.maps[i] < offset {
				continue
			}
			if b.maps[i] > offset {
				break
			}
			s := int(scores[uint32(offset)])
			switch joinType {
			case JoinAll:
				if s != len(vs) {
					continue
				}
			case JoinMajor:
				if s < len(vs)/2 {
					continue
				}
			}
			*res = append(*res, KeyTimeScore{
				Key:      b.keys[i],
				UnixDeci: (b.baseTime + int64(b.maps[i])),
				Score:    s,
			})
		}
	}
}

func Unmarshal(rd io.Reader) (*Day, error) {
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
	h := crc32.NewIEEE()
	rd = io.TeeReader(rd, h)

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

	verify := h.Sum32()
	var checksum uint32
	if err := binary.Read(rd, binary.BigEndian, &checksum); err != nil {
		return nil, fmt.Errorf("read checksum: %v", err)
	}
	if checksum != verify {
		return nil, fmt.Errorf("invalid checksum %x and %x", verify, checksum)
	}
	return b, nil
}

func (b *Day) MarshalBinary() []byte {
	p := &bytes.Buffer{}
	b.Marshal(p)
	return p.Bytes()
}

func (b *Day) Marshal(w io.Writer) (int, error) {
	w = &meterWriter{Writer: w}
	w.Write([]byte{1})
	if err := binary.Write(w, binary.BigEndian, b.baseTime); err != nil {
		return 0, err
	}
	for _, h := range b.hours {
		if err := h.writeTo(w); err != nil {
			return 0, err
		}
	}
	return w.(*meterWriter).size, nil
}

func (b *hourMap) writeTo(w io.Writer) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	h := crc32.NewIEEE()
	w = io.MultiWriter(w, h)
	if err := binary.Write(w, binary.BigEndian, b.baseTime); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, b.hashNum); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, b.table.GetSerializedSizeInBytes()); err != nil {
		return err
	}
	if _, err := b.table.WriteTo(w); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(b.keys))); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, b.keys); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, b.maps); err != nil {
		return err
	}
	// Write CRC32 checksum to the end of stream.
	return binary.Write(w, binary.BigEndian, h.Sum32())
}

func (b *Day) RoughSizeBytes() (sz int64) {
	for i := range b.hours {
		sz += int64(b.hours[i].table.GetSerializedSizeInBytes())
		sz += int64(len(b.hours[i].keys)) * 10
	}
	return
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
