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
	hour10 = 600 * 10
	day    = 86400
)

type Day struct {
	mfmu     sync.Mutex
	baseTime int64
	hours    [day * 10 / hour10]*hourMap
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

func (b *Day) BaseTimeDeci() int64 {
	return b.baseTime
}

func (b *Day) EndTimeDeci() int64 {
	return b.baseTime + 86400*10 - 1
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
		h := h16(v, b.baseTime)
		for i := 0; i < int(b.hashNum); i++ {
			b.table.Add(h[i] + offset)
		}
	}
}

func (b *Day) Join(hashes []uint32, startDeci int64, count int, joinType int) (res []KeyTimeScore) {
	startSlot := int((startDeci - b.baseTime) / hour10)
	for i := startSlot; i >= 0; i-- {
		startOffset := startDeci - b.hours[i].baseTime + 1
		if startOffset > hour10 {
			startOffset = hour10
		}
		b.hours[i].join(hashes, i, startOffset, count, joinType, &res)
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

func (b *hourMap) join(vs []uint32, hr int, limit int64, count int, joinType int, res *[]KeyTimeScore) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	type hashState struct {
		h uint32
		roaring.Bitmap
	}

	hashes := map[uint32]*hashState{}
	hashSort := []*hashState{}
	vsHashes := [][4]uint32{}
	for _, v := range vs {
		h := h16(v, b.baseTime)
		for i := 0; i < int(b.hashNum); i++ {
			x := &hashState{h: h[i]}
			hashes[h[i]] = x
			hashSort = append(hashSort, x)
		}
		vsHashes = append(vsHashes, h)
	}
	sort.Slice(hashSort, func(i, j int) bool { return hashSort[i].h < hashSort[j].h })

SEARCH:
	overlapHashes := []*hashState{}
	for iter := b.table.Iterator(); len(hashSort) > 0; {
		hs := hashSort[0]
		h := hs.h
		hashSort = hashSort[1:]

		iter.AdvanceIfNeeded(h)
		for iter.HasNext() {
			h2 := iter.PeekNext()
			if h2-h < uint32(limit) {
				// The next value (h2), is not only within the [0, hour10) range of current hash,
				// but also the start of the next hash in 'hashSort'. Dealing 2 hashes together is hard,
				// so remove the next hash and store it elsewhere. It will be checked in the next round.
				if len(hashSort) > 0 && h2 == hashSort[0].h {
					overlapHashes = append(overlapHashes, hashSort[0])
					hashSort = hashSort[1:]
				}

				hs.Add(h2 - h)
				iter.Next()
			} else {
				break
			}
		}
	}
	if len(overlapHashes) > 0 {
		// fmt.Println("overlap", hr, overlapHashes)
		hashSort = overlapHashes
		goto SEARCH
	}

	// z := time.Now()
	scores := map[uint32]int{}
	final := roaring.New()
	for i := range vsHashes {
		raw := vsHashes[i]
		m := &hashes[raw[0]].Bitmap
		for i := 1; i < int(b.hashNum); i++ {
			m.And(&hashes[raw[i]].Bitmap)
		}
		if joinType == JoinAll {
			final.And(m)
		} else {
			m.Iterate(func(x uint32) bool { scores[x]++; return true })
			final.Or(m)
		}
	}
	// if final.GetCardinality() > 0 {
	// 	fmt.Println(final.GetCardinality(), time.Since(z))
	// }

	for iter, i := final.Iterator(), 0; iter.HasNext(); {
		offset := uint16(iter.Next())
		s := int(scores[uint32(offset)])
		for ; i < len(b.keys); i++ {
			if b.maps[i] < offset {
				continue
			}
			if b.maps[i] > offset {
				break
			}
			switch joinType {
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
		b.baseTime/10, time.Unix(b.baseTime/10, 0).Format("01-02;15:04"), b.hashNum)
	if len(b.keys) > 0 {
		fmt.Fprintf(buf, "keys: %d (last=%016x-->%d), ",
			len(b.keys), b.keys[len(b.keys)-1], b.maps[len(b.maps)-1])
	} else {
		fmt.Fprintf(buf, "keys: none, ")
	}
	fmt.Fprintf(buf, "table: %d (size=%db)", b.table.GetCardinality(), b.table.GetSerializedSizeInBytes())
}
