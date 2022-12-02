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
	offsets      []uint16        // key -> ts offset maps
}

func newHourMap(baseTime int64, hashNum int8) *hourMap {
	m := &hourMap{
		baseTimeDeci: baseTime / hour10 * hour10,
		table:        roaring.New(),
	}
	return m
}

func (b *Day) Add(key uint64, v []uint32) error {
	return b.AddWithTime(key, clock.UnixDeci(), v)
}

func (b *Day) AddWithTime(key uint64, unixDeci int64, vs []uint32) error {
	b.addFast(unixDeci/10, vs)
	return b.hours[(unixDeci-b.baseTime*10)/hour10].add(unixDeci, key, vs)
}

func (b *hourMap) add(unixDeci int64, key uint64, vs []uint32) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if unixDeci-b.baseTimeDeci > hour10 || unixDeci < b.baseTimeDeci {
		return fmt.Errorf("invalid timestamp: %v, base: %v", unixDeci, b.baseTimeDeci)
	}

	offset := uint32(unixDeci - b.baseTimeDeci)

	if len(b.offsets) > 0 {
		// if key <= b.keys[len(b.keys)-1] {
		// 	panic(fmt.Sprintf("invalid key: %016x, last key: %016x", key, b.keys[len(b.keys)-1]))
		// }
		if uint16(offset) < b.offsets[len(b.offsets)-1] {
			return fmt.Errorf("invalid timestamp: %d, last: %d",
				b.baseTimeDeci+int64(offset), b.baseTimeDeci+int64(b.offsets[len(b.keys)-1]))
		}
	}

	b.keys = append(b.keys, key)
	b.offsets = append(b.offsets, uint16(offset))

	for _, v := range vs {
		h := h16(v, b.baseTimeDeci)
		b.table.Add(offset<<16 | h[0]&0xffff)
	}
	return nil
}

func (b *Day) Join(qs, musts []uint32, startDeci int64, count int, joinType int) (res []KeyTimeScore) {
	qs, musts = dedupUint32(qs, musts)
	fast := b.joinFast(qs, musts, joinType)

	startSlot := int((startDeci - b.baseTime*10) / hour10)
	scoresMap := make([]uint8, hour10)

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
		var mustHashSort []uint32
		for _, v := range qs {
			h := h16(v, m.baseTimeDeci)
			hashSort = append(hashSort, h[0]&0xffff)
		}
		for _, v := range musts {
			h := h16(v, m.baseTimeDeci)
			mustHashSort = append(mustHashSort, h[0]&0xffff)
		}
		sort.Slice(hashSort, func(i, j int) bool { return hashSort[i] < hashSort[j] })
		sort.Slice(mustHashSort, func(i, j int) bool { return mustHashSort[i] < mustHashSort[j] })

		for i := range scoresMap {
			scoresMap[i] = 0
		}
		m.join(scoresMap, hashSort, mustHashSort, i, &fast, uint32(startOffset), joinType, count, &res)
		if count > 0 && len(res) >= count {
			break
		}
	}
	//sort.Slice(res, func(i, j int) bool {
	//	if res[i].UnixDeci == res[j].UnixDeci {
	//		return res[i].Key > res[j].Key
	//	}
	//	return res[i].UnixDeci > res[j].UnixDeci
	//})
	return
}

func (b *hourMap) join(scoresMap []uint8,
	hashSort, mustHashSort []uint32, hr int, fast *bitmap1440,
	limit uint32, joinType int, count int, res *[]KeyTimeScore) {

	var num int
	if !func() (found bool) {
		b.mu.RLock()
		defer b.mu.RUnlock()

		iter := b.table.Iterator()
		for i := uint32(0); i < limit; i++ {
			if !fast.contains(hr, i) {
				continue
			}
			for _, hs := range hashSort {
				x := uint32(i)<<16 | hs
				iter.AdvanceIfNeeded(x)
				if iter.HasNext() && iter.PeekNext() == x {
					scoresMap[i]++
					found = true
				}
			}
		}

		if len(mustHashSort) > 0 {
			iter := b.table.Iterator()
			for i := uint32(0); i < limit; i++ {
				if !fast.contains(hr, i) {
					continue
				}
				for _, hs := range mustHashSort {
					x := uint32(i)<<16 | hs
					iter.AdvanceIfNeeded(x)
					if iter.HasNext() && iter.PeekNext() == x {
						scoresMap[i]++
						found = true
					} else {
						scoresMap[i] = 0
					}
				}
			}
		}
		num = len(b.offsets)
		return
	}() {
		return
	}

	for i, offset := num-1, len(scoresMap)-1; ; offset-- {
		for offset > 0 && scoresMap[offset] == 0 {
			offset--
		}
		if offset < 0 {
			break
		}
		s := int(scoresMap[offset])
		for ; i >= 0; i-- {
			if b.offsets[i] > uint16(offset) {
				continue
			}
			if b.offsets[i] < uint16(offset) {
				break
			}
			if joinType == JoinMajor && s < majorScore(len(hashSort))+len(mustHashSort) {
				continue
			}
			if joinType == JoinAll && s < len(hashSort)+len(mustHashSort) {
				continue
			}
			*res = append(*res, KeyTimeScore{
				Key:      b.keys[i],
				UnixDeci: (b.baseTimeDeci + int64(b.offsets[i])),
				Score:    s,
			})
		}
		if len(*res) > count {
			break
		}
	}
}

func (b *Day) Clone() *Day {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b2 := &Day{}
	b2.baseTime = b.baseTime
	b2.hashNum = b.hashNum
	b2.fastTable = b.fastTable.Clone()
	for i := range b2.hours {
		b2.hours[i] = b.hours[i].clone()
	}
	return b2
}

func (b *hourMap) clone() *hourMap {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b2 := &hourMap{}
	b2.baseTimeDeci = b.baseTimeDeci
	b2.table = b.table.Clone()
	b2.keys = b.keys
	b2.offsets = b.offsets
	return b2
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

	b.offsets = make([]uint16, keysLen)
	if err := binary.Read(rd, binary.BigEndian, b.offsets); err != nil {
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
	b.mu.RLock()
	defer b.mu.RUnlock()

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
	if err := binary.Write(w, binary.BigEndian, b.offsets); err != nil {
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
	fmt.Fprintf(buf, "[%v] bf hash: %d, fast table: %d (size=%db)\n",
		time.Unix(b.baseTime, 0).Format("2006-01-02Z07"), b.hashNum,
		b.fastTable.GetCardinality(), b.fastTable.GetSerializedSizeInBytes())
	keys, cards := 0, 0
	for _, h := range b.hours {
		k, c := h.debug(buf)
		keys += k
		cards += c
		buf.WriteByte('\n')
	}
	fmt.Fprintf(buf, "[%v] total keys: %d, total hashes: %d",
		time.Unix(b.baseTime, 0).Format("2006-01-02Z07"), keys, cards)
	return buf.String()
}

func (b *hourMap) debug(buf io.Writer) (int, int) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	fmt.Fprintf(buf, "[%d;%v] ",
		b.baseTimeDeci/10, time.Unix(b.baseTimeDeci/10, 0).Format("15"))
	if len(b.keys) > 0 {
		fmt.Fprintf(buf, "keys: %d (last=%016x-->%d), ",
			len(b.keys), b.keys[len(b.keys)-1], b.offsets[len(b.offsets)-1])
	} else {
		fmt.Fprintf(buf, "keys: none, ")
	}
	card := b.table.GetCardinality()
	fmt.Fprintf(buf, "table: %d (size=%db)", card, b.table.GetSerializedSizeInBytes())
	return len(b.keys), int(card)
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

func (b *Day) joinFast(qs, musts []uint32, joinType int) (res bitmap1440) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	type hashState struct {
		h uint32
		roaring.Bitmap
	}

	hashStates := map[uint32]*hashState{}
	var hashSort []*hashState
	var qsHashes [][4]uint32
	var mustHashes [][4]uint32

	for _, v := range musts {
		h := h16(v, b.baseTime)
		for i := 0; i < int(b.hashNum); i++ {
			x := &hashState{h: h[i]}
			hashStates[h[i]] = x // TODO: duplicated hashes
			hashSort = append(hashSort, x)
		}
		mustHashes = append(mustHashes, h)
	}

	for _, v := range qs {
		h := h16(v, b.baseTime)
		for i := 0; i < int(b.hashNum); i++ {
			x := &hashState{h: h[i]}
			hashStates[h[i]] = x
			hashSort = append(hashSort, x)
		}
		qsHashes = append(qsHashes, h)
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
	for i := range qsHashes {
		raw := qsHashes[i]
		m := &hashStates[raw[0]].Bitmap
		for i := 1; i < int(b.hashNum); i++ {
			m.And(&hashStates[raw[i]].Bitmap)
		}
		if joinType == JoinAll {
			if i == 0 {
				final = m
			} else {
				final.And(m)
			}
		} else {
			m.Iterate(func(x uint32) bool { scores[x]++; return true })
			final.Or(m)
		}
	}

	for i := range mustHashes {
		raw := mustHashes[i]
		m := &hashStates[raw[0]].Bitmap
		for i := 1; i < int(b.hashNum); i++ {
			m.And(&hashStates[raw[i]].Bitmap)
		}
		m.Iterate(func(x uint32) bool { scores[x]++; return true })
		if len(qsHashes) == 0 && i == 0 {
			final = m
		} else {
			final.And(m)
		}
	}

	for iter := final.Iterator(); iter.HasNext(); {
		offset := uint16(iter.Next())
		s := int(scores[uint32(offset)])
		if joinType == JoinMajor && s < majorScore(len(qs))+len(musts) {
			continue
		}
		res.add(offset)
	}
	return
}
