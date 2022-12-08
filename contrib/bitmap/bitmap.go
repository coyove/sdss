package bitmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/pierrec/lz4"
)

const (
	slotSize     = 1 << 14
	slotNum      = 1 << 6
	fastSlotNum  = 1 << 10
	fastSlotSize = slotNum * slotSize / fastSlotNum
)

type Range struct {
	mu         sync.RWMutex
	mfmu       sync.Mutex
	start, end int64
	hashNum    int8
	fastTable  *roaring.Bitmap
	slots      [slotNum]*subMap
}

func New(start int64, hashNum int8) *Range {
	d := &Range{
		start:     start,
		end:       -1,
		hashNum:   hashNum,
		fastTable: roaring.New(),
	}
	for i := range d.slots {
		d.slots[i] = &subMap{}
	}
	return d
}

func (b *Range) Start() int64 {
	return b.start
}

func (b *Range) End() int64 {
	return b.start + b.end
}

func (b *Range) FirstKey() uint64 {
	if b.end < 0 {
		return 0
	}
	m := b.slots[0]
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.keys[0]
}

func (b *Range) LastKey() uint64 {
	if b.end < 0 {
		return 0
	}
	m := b.slots[b.end/slotSize]
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.keys[len(m.keys)-1]
}

type subMap struct {
	mu    sync.RWMutex
	keys  []uint64
	spans []uint32
	xfs   []byte
}

func (b *Range) Add(key uint64, v []uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.end == slotSize*slotNum-1 {
		return false
	}

	b.end++
	offset := uint32(b.end / fastSlotSize)
	for _, v := range v {
		h := h16(uint32(v), b.start)
		for i := 0; i < int(b.hashNum); i++ {
			b.fastTable.Add(h[i])
			b.fastTable.Add(h[i] + 1 + offset)
		}
	}

	m := b.slots[b.end/slotSize]
	m.mu.Lock()
	defer m.mu.Unlock()

	xf := xfNew(v)

	m.keys = append(m.keys, key)
	m.xfs = append(m.xfs, xf...)
	if len(m.spans) == 0 {
		m.spans = append(m.spans, uint32(len(xf)))
	} else {
		m.spans = append(m.spans, m.spans[len(m.spans)-1]+uint32(len(xf)))
	}
	return true
}

func (b *Range) Join(qs, musts []uint64, start int64, count int, joinType int) (res []KeyTimeScore) {
	qs, musts = dedupUint64(qs, musts)
	fast := b.joinFast(qs, musts, joinType)

	if start == -1 {
		start = b.end
	} else {
		start -= b.start
		if start < 0 || start >= slotNum*slotSize {
			return
		}
	}

	startSlot := int(start / slotSize)
	scoresMap := make([]uint8, slotSize)

	for i := startSlot; i >= 0; i-- {
		if fast[i] == 0 {
			continue
		}
		startOffset := start - int64(i*slotSize) + 1
		if startOffset >= slotSize {
			startOffset = slotSize
		}

		m := b.slots[i]
		for i := range scoresMap {
			scoresMap[i] = 0
		}
		m.join(scoresMap, qs, musts, i, &fast, startOffset, joinType, count, &res)
		if count > 0 && len(res) >= count {
			break
		}
	}

	for i := range res {
		res[i].Id += b.start
	}
	return
}

func (b *subMap) prevSpan(i int64) uint32 {
	if i == 0 {
		return 0
	}
	return b.spans[i-1]
}

func (b *subMap) join(scoresMap []uint8,
	hashSort, mustHashSort []uint64, hr int, fast *bitmap1024,
	end int64, joinType int, count int, res *[]KeyTimeScore) {

	b.mu.RLock()
	defer b.mu.RUnlock()

	end1 := end - 1
	if end1 >= int64(len(b.keys)) {
		end1 = int64(len(b.keys)) - 1
	}

	for i := end1; i >= 0; i-- {
		if !fast.contains(uint16((hr*slotSize + int(i)) / fastSlotSize)) {
			continue
		}
		s := 0
		xf := xfBuild(b.xfs[b.prevSpan(i):b.spans[i]])
		for _, hs := range hashSort {
			if xf.Contains(hs) {
				s++
			}
		}
		for _, hs := range mustHashSort {
			if xf.Contains(hs) {
				s++
			} else {
				goto NEXT
			}
		}
		if joinType == JoinMajor && s < majorScore(len(hashSort))+len(mustHashSort) {
			goto NEXT
		}
		if joinType == JoinAll && s < len(hashSort)+len(mustHashSort) {
			goto NEXT
		}
		*res = append(*res, KeyTimeScore{
			Key:   b.keys[i],
			Id:    int64(hr*slotSize) + int64(i),
			Score: s,
		})
		if len(*res) >= count {
			break
		}
	NEXT:
	}
}

func (b *Range) Clone() *Range {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b2 := &Range{}
	b2.start = b.start
	b2.end = b.end
	b2.hashNum = b.hashNum
	b2.fastTable = b.fastTable.Clone()
	for i := range b2.slots {
		b2.slots[i] = b.slots[i].clone()
	}
	return b2
}

func (b *subMap) clone() *subMap {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return &subMap{
		keys:  b.keys,
		spans: b.spans,
		xfs:   b.xfs,
	}
}

func Unmarshal(rd io.Reader) (*Range, error) {
	var err error
	var ver byte
	if err := binary.Read(rd, binary.BigEndian, &ver); err != nil {
		return nil, fmt.Errorf("read version: %v", err)
	}
	if ver == 4 {
		rd = lz4.NewReader(rd)
	}

	b := &Range{}
	h := crc32.NewIEEE()
	rd = io.TeeReader(rd, h)

	if err := binary.Read(rd, binary.BigEndian, &b.start); err != nil {
		return nil, fmt.Errorf("read start: %v", err)
	}

	if err := binary.Read(rd, binary.BigEndian, &b.end); err != nil {
		return nil, fmt.Errorf("read end: %v", err)
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

	for i := range b.slots {
		b.slots[i], err = readSubMap(rd)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func readSubMap(rd io.Reader) (*subMap, error) {
	h := crc32.NewIEEE()
	rd = io.TeeReader(rd, h)

	b := &subMap{}

	var keysLen uint32
	if err := binary.Read(rd, binary.BigEndian, &keysLen); err != nil {
		return nil, fmt.Errorf("read keys length: %v", err)
	}

	b.keys = make([]uint64, keysLen)
	if err := binary.Read(rd, binary.BigEndian, b.keys); err != nil {
		return nil, fmt.Errorf("read keys: %v", err)
	}

	b.spans = make([]uint32, keysLen)
	if err := binary.Read(rd, binary.BigEndian, b.spans); err != nil {
		return nil, fmt.Errorf("read spans: %v", err)
	}

	if len(b.spans) > 0 {
		b.xfs = make([]byte, b.spans[len(b.spans)-1])
		if err := binary.Read(rd, binary.BigEndian, b.xfs); err != nil {
			return nil, fmt.Errorf("read xfs: %v", err)
		}
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

func (b *Range) MarshalBinary() []byte {
	p := &bytes.Buffer{}
	b.Marshal(p)
	return p.Bytes()
}

func (b *Range) Marshal(w io.Writer) (int, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	mw := &meterWriter{Writer: w}
	mw.Write([]byte{4})
	zw := lz4.NewWriter(mw)

	h := crc32.NewIEEE()
	w = io.MultiWriter(zw, h)

	if err := binary.Write(w, binary.BigEndian, b.start); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.BigEndian, b.end); err != nil {
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
	for _, h := range b.slots {
		if err := h.writeTo(w); err != nil {
			return 0, err
		}
	}
	if err := zw.Close(); err != nil {
		return 0, err
	}
	return mw.size, nil
}

func (b *subMap) writeTo(w io.Writer) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	h := crc32.NewIEEE()
	w = io.MultiWriter(w, h)
	if err := binary.Write(w, binary.BigEndian, uint32(len(b.keys))); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, b.keys); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, b.spans); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, b.xfs); err != nil {
		return err
	}
	// Write CRC32 checksum to the end of stream.
	return binary.Write(w, binary.BigEndian, h.Sum32())
}

func (b *Range) RoughSizeBytes() (sz int64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	sz += int64(b.fastTable.GetSerializedSizeInBytes())
	for i := range b.slots {
		sz += int64(len(b.slots[i].xfs))
		sz += int64(len(b.slots[i].keys)) * 12
	}
	return
}

func (b *Range) String() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "[%08d] tail end: %d, bf hash: %d, fast table: %d (size=%db), rough size: %db\n",
		b.start, b.end, b.hashNum,
		b.fastTable.GetCardinality(), b.fastTable.GetSerializedSizeInBytes(), b.RoughSizeBytes())
	for i, h := range b.slots {
		h.debug(i, buf)
	}
	return buf.String()
}

func (b *subMap) debug(i int, buf io.Writer) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.keys) > 0 {
		fmt.Fprintf(buf, "[%02d;0x%05x] ", i, i*slotSize)
		fmt.Fprintf(buf, "keys: %5d/%2d, last key: %016x, filter size: %db\n",
			len(b.keys), len(b.keys)/fastSlotSize, b.keys[len(b.keys)-1], len(b.xfs))
	}
}

func (b *Range) joinFast(qs, musts []uint64, joinType int) (res bitmap1024) {
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
		h := h16(uint32(v), b.start)
		for i := 0; i < int(b.hashNum); i++ {
			x := &hashState{h: h[i]}
			hashStates[h[i]] = x // TODO: duplicated hashes
			hashSort = append(hashSort, x)
		}
		mustHashes = append(mustHashes, h)
	}

	for _, v := range qs {
		h := h16(uint32(v), b.start)
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
			if h2-(h+1) < fastSlotSize {
				// The next value (h2), is not only within the [0, fastSlotSize] range of current hash,
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

func (b *Range) Find(key uint64) (int64, func(uint64) bool) {
	for hr, m := range b.slots {
		m.mu.Lock()
		for i, k := range m.keys {
			if k == key {
				x := xfBuild(m.xfs[m.prevSpan(int64(i)):m.spans[i]])
				m.mu.Unlock()
				return int64(hr)*slotSize + int64(i), x.Contains
			}
		}
		m.mu.Unlock()
	}
	return 0, nil
}
