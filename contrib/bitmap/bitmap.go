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
	mu        sync.RWMutex
	mfmu      sync.Mutex
	baseTime  int64 // rounded to 86400 (1 day)
	hashNum   int8
	fastTable *roaring.Bitmap
	hours     [24]*hourMap
}

func New(baseTime int64, hashNum int8) *Day {
	d := &Day{
		baseTime:  baseTime / day * day,
		hashNum:   hashNum,
		fastTable: roaring.New(),
	}
	for i := range d.hours {
		d.hours[i] = newHourMap(d.baseTime*10+hour10*int64(i), hashNum)
	}
	return d
}

func (b *Day) BaseTime() int64 {
	return b.baseTime
}

func (b *Day) BaseTimeDeci() int64 {
	return b.baseTime * 10
}

func (b *Day) EndTimeDeci() int64 {
	return b.BaseTimeDeci() + 86400*10 - 1
}

type hourMap struct {
	mu           sync.RWMutex
	baseTimeDeci int64           // baseTime + 12 bits ts = final timestamp
	table        *roaring.Bitmap // 20 bits hash + 12 bits ts
	keys         []uint64        // keys
	maps         []uint16        // key -> ts offset maps
}

func newHourMap(baseTime int64, hashNum int8) *hourMap {
	m := &hourMap{
		baseTimeDeci: baseTime / hour10 * hour10,
		table:        roaring.New(),
	}
	return m
}

func (b *Day) Add(key uint64, v []uint32) {
	b.addWithTime(key, clock.UnixDeci(), v)
}

func (b *Day) addWithTime(key uint64, unixDeci int64, vs []uint32) {
	b.addFast(unixDeci/10, vs)
	b.hours[(unixDeci-b.baseTime*10)/hour10].add(unixDeci, key, vs)
}

func (b *hourMap) add(unixDeci int64, key uint64, vs []uint32) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if unixDeci-b.baseTimeDeci > hour10 || unixDeci < b.baseTimeDeci {
		panic(fmt.Sprintf("invalid timestamp: %v, base: %v", unixDeci, b.baseTimeDeci))
	}

	offset := uint32(unixDeci - b.baseTimeDeci)

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
		h := h16(v, b.baseTimeDeci)
		b.table.Add(offset<<16 | h[0]&0xffff)
	}
}

func (b *Day) Join(hashes []uint32, startDeci int64, count int, joinType int) (res []KeyTimeScore) {
	fast := b.joinFast(hashes, joinType)
	startSlot := int((startDeci - b.baseTime*10) / hour10)
	for i := startSlot; i >= 0; i-- {
		if fast[i] == 0 {
			continue
		}
		startOffset := startDeci - b.hours[i].baseTimeDeci + 1
		if startOffset > hour10 {
			startOffset = hour10
		}

		m := b.hours[i]
		var hashSort []uint32
		for _, v := range hashes {
			h := h16(v, m.baseTimeDeci)
			hashSort = append(hashSort, h[0]&0xffff)
		}
		sort.Slice(hashSort, func(i, j int) bool { return hashSort[i] < hashSort[j] })

		m.join(hashes, hashSort, i, &fast, 0, uint32(startOffset), count, joinType, &res)
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

func (b *hourMap) join(vs []uint32, hashSort []uint32, hr int, fast *bitmap1440,
	start, limit uint32, count int, joinType int, res *[]KeyTimeScore) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	scores := map[uint32]int{}
	final := roaring.New()

	iter := b.table.Iterator()
	for i := start; i < limit; i++ {
		if !fast.contains(hr, i) {
			continue
		}
		for _, hs := range hashSort {
			x := uint32(i)<<16 | hs
			iter.AdvanceIfNeeded(x)
			if iter.HasNext() && iter.PeekNext() == x {
				final.Add(uint32(i))
				scores[uint32(i)]++
			}
		}
	}

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
			if joinType == JoinMajor && s < len(vs)/2 {
				continue
			}
			*res = append(*res, KeyTimeScore{
				Key:      b.keys[i],
				UnixDeci: (b.baseTimeDeci + int64(b.maps[i])),
				Score:    s,
			})
		}
	}
}

func Unmarshal(rd io.Reader) (*Day, error) {
	var err error
	var ver byte
	if err := binary.Read(rd, binary.BigEndian, &ver); err != nil {
		return nil, fmt.Errorf("read version: %v", err)
	}

	b := &Day{}
	h := crc32.NewIEEE()
	rd = io.TeeReader(rd, h)

	if err := binary.Read(rd, binary.BigEndian, &b.baseTime); err != nil {
		return nil, fmt.Errorf("read baseTime: %v", err)
	}

	if err := binary.Read(rd, binary.BigEndian, &b.hashNum); err != nil {
		return nil, fmt.Errorf("read hashNum: %v", err)
	}

	var topSize uint64
	if err := binary.Read(rd, binary.BigEndian, &topSize); err != nil {
		return nil, fmt.Errorf("read fast table bitmap size: %v", err)
	}

	b.fastTable = roaring.New()
	if _, err := b.fastTable.ReadFrom(io.LimitReader(rd, int64(topSize))); err != nil {
		return nil, fmt.Errorf("read fast table bitmap: %v", err)
	}

	verify := h.Sum32()
	var checksum uint32
	if err := binary.Read(rd, binary.BigEndian, &checksum); err != nil {
		return nil, fmt.Errorf("read checksum: %v", err)
	}
	if checksum != verify {
		return nil, fmt.Errorf("invalid fast table checksum %x and %x", verify, checksum)
	}
	if err != nil {
		return nil, fmt.Errorf("read fast table: %v", err)
	}

	for i := range b.hours {
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
	if err := binary.Read(rd, binary.BigEndian, &b.baseTimeDeci); err != nil {
		return nil, fmt.Errorf("read baseTime: %v", err)
	}

	// if err := binary.Read(rd, binary.BigEndian, &b.hashNum); err != nil {
	// 	return nil, fmt.Errorf("read hashNum: %v", err)
	// }

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
	mw := &meterWriter{Writer: w}
	mw.Write([]byte{1})

	h := crc32.NewIEEE()
	w = io.MultiWriter(mw, h)
	if err := binary.Write(w, binary.BigEndian, b.baseTime); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.BigEndian, b.hashNum); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.BigEndian, b.fastTable.GetSerializedSizeInBytes()); err != nil {
		return 0, err
	}
	if _, err := b.fastTable.WriteTo(w); err != nil {
		return 0, err
	}
	// Write CRC32 checksum to the end of stream.
	if err := binary.Write(w, binary.BigEndian, h.Sum32()); err != nil {
		return 0, err
	}
	for _, h := range b.hours {
		if err := h.writeTo(w); err != nil {
			return 0, err
		}
	}
	return mw.size, nil
}

func (b *hourMap) writeTo(w io.Writer) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	h := crc32.NewIEEE()
	w = io.MultiWriter(w, h)
	if err := binary.Write(w, binary.BigEndian, b.baseTimeDeci); err != nil {
		return err
	}
	// if err := binary.Write(w, binary.BigEndian, b.hashNum); err != nil {
	// 	return err
	// }
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
	sz += int64(b.fastTable.GetSerializedSizeInBytes())
	for i := range b.hours {
		sz += int64(b.hours[i].table.GetSerializedSizeInBytes())
		sz += int64(len(b.hours[i].keys)) * 10
	}
	return
}

func (b *Day) String() string {
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "[%d;%v] fast table: %d (size=%db)\n",
		b.baseTime, time.Unix(b.baseTime, 0).Format("01-02"),
		b.fastTable.GetCardinality(), b.fastTable.GetSerializedSizeInBytes())
	for _, h := range b.hours {
		h.debug(buf)
		buf.WriteByte('\n')
	}
	return buf.String()
}

func (b *hourMap) debug(buf io.Writer) {
	fmt.Fprintf(buf, "[%d;%v] ",
		b.baseTimeDeci/10, time.Unix(b.baseTimeDeci/10, 0).Format("15:04"))
	if len(b.keys) > 0 {
		fmt.Fprintf(buf, "keys: %d (last=%016x-->%d), ",
			len(b.keys), b.keys[len(b.keys)-1], b.maps[len(b.maps)-1])
	} else {
		fmt.Fprintf(buf, "keys: none, ")
	}
	fmt.Fprintf(buf, "table: %d (size=%db)", b.table.GetCardinality(), b.table.GetSerializedSizeInBytes())
}

func (b *Day) addFast(ts int64, vs []uint32) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ts-b.baseTime > day || ts < b.baseTime {
		panic(fmt.Sprintf("invalid timestamp: %v, base: %v", ts, b.baseTime))
	}

	offset := uint32(ts-b.baseTime) / 60

	for _, v := range vs {
		h := h16(v, b.baseTime)
		for i := 0; i < int(b.hashNum); i++ {
			b.fastTable.Add(h[i])
			b.fastTable.Add(h[i] + 1 + offset)
		}
	}
}

func (b *Day) joinFast(vs []uint32, joinType int) (res bitmap1440) {
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
	for iter := b.fastTable.Iterator(); len(hashSort) > 0; {
		hs := hashSort[0]
		h := hs.h
		hashSort = hashSort[1:]

		iter.AdvanceIfNeeded(h)
		if !iter.HasNext() {
			continue
		}
		if iter.Next() != h {
			continue
		}
		for iter.HasNext() {
			h2 := iter.PeekNext()
			if h2-(h+1) < 1440 {
				// The next value (h2), is not only within the [0, 1440] range of current hash,
				// but also the start of the next hash in 'hashSort'. Dealing 2 hashes together is hard,
				// so remove the next hash and store it elsewhere. It will be checked in the next round.
				if len(hashSort) > 0 && h2 == hashSort[0].h {
					overlapHashes = append(overlapHashes, hashSort[0])
					hashSort = hashSort[1:]
				}

				hs.Add(h2 - (h + 1))
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

	for iter := final.Iterator(); iter.HasNext(); {
		offset := uint16(iter.Next())
		s := int(scores[uint32(offset)])
		if joinType == JoinMajor && s < len(vs)/2 {
			continue
		}
		res.add(offset)
	}
	return
}
