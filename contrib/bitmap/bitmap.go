package bitmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"time"

	"github.com/coyove/sdss/contrib/roaring"
	"github.com/pierrec/lz4"
)

const (
	slotSize        = 1 << 14
	slotNum         = 1 << 6
	fastSlotNum     = 1 << 12
	fastSlotSize    = 1 << 8
	fastSlotMask    = 0xfffff000
	highEntropyMask = 0xffff0000

	Capcity = slotSize * slotNum
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

func (b *Range) SetStart(v int64) {
	b.start = v
}

func (b *Range) Start() int64 {
	return b.start
}

func (b *Range) End() int64 {
	return b.start + b.end
}

func (b *Range) Len() int64 {
	return b.end + 1
}

func (b *Range) FirstKey() Key {
	if b.end < 0 {
		return Key{}
	}
	m := b.slots[0]
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.keys[0]
}

func (b *Range) LastKey() Key {
	if b.end < 0 {
		return Key{}
	}
	m := b.slots[b.end/slotSize]
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.keys[len(m.keys)-1]
}

type subMap struct {
	mu    sync.RWMutex
	keys  []Key
	spans []uint32
	xfs   []byte
}

func (b *Range) Add(key Key, values []uint64) bool {
	return b.AddHighEntropy(key, values, nil)
}

func (b *Range) AddHighEntropy(key Key, values, heValues []uint64) bool {
	if len(values)+len(heValues) == 0 {
		panic("empty values")
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.end == slotSize*slotNum-1 {
		return false
	}

	b.end++
	offset := uint32(b.end / fastSlotSize)
	for _, v := range values {
		h := h16(uint32(v), b.start)
		for i := 0; i < int(b.hashNum); i++ {
			b.fastTable.Add(h[i]&fastSlotMask | offset)
		}
	}

	for _, v := range heValues {
		// For high entropy values, we only store their high 16 bits into fastTable.
		h := h16(uint32(v), b.start)
		for i := 0; i < int(b.hashNum); i++ {
			b.fastTable.Add(h[i]&highEntropyMask | offset)
		}
	}

	values = append(values, heValues...)

	m := b.slots[b.end/slotSize]
	m.mu.Lock()
	defer m.mu.Unlock()

	xf := xfNew(values)

	m.keys = append(m.keys, key)
	m.xfs = append(m.xfs, xf...)
	if len(m.spans) == 0 {
		m.spans = append(m.spans, uint32(len(xf)))
	} else {
		m.spans = append(m.spans, m.spans[len(m.spans)-1]+uint32(len(xf)))
	}
	return true
}

func (b *Range) Join(vs Values, start int64, desc bool, f func(KeyIdScore) bool) (jm JoinMetrics) {
	vs.clean()
	fastStart := time.Now()
	fast := b.joinFast(&vs)
	jm.FastElapsed = time.Since(fastStart)
	jm.Start = b.start

	if start == -1 {
		start = b.end
	} else {
		start -= b.start
		if start < 0 || start >= slotNum*slotSize {
			return
		}
	}

	startSlot := int(start / slotSize)
	vs.Exact = append(vs.Exact, vs.HighEntropy...)

	endSlot, endCmp, step := -1, 1, -1
	if !desc {
		endSlot, endCmp, step = slotNum, -1, 1
	}

	for i := startSlot; icmp(int64(i), int64(endSlot)) == endCmp; i += step {
		if fast[i] == 0 {
			continue
		}

		m := b.slots[i]
		startOffset := start - int64(i*slotSize)
		if startOffset >= int64(len(m.keys)) {
			startOffset = int64(len(m.keys)) - 1
		}
		if startOffset < 0 {
			startOffset = 0
		}
		if exit := m.join(vs, i, &fast, startOffset, desc, b.start, &jm, f); exit {
			break
		}
	}

	jm.Elapsed = time.Since(fastStart)
	return
}

func (b *subMap) prevSpan(i int64) uint32 {
	if i == 0 {
		return 0
	}
	return b.spans[i-1]
}

func (b *subMap) join(v Values, hr int, fast *bitmap1024, end1 int64, desc bool,
	baseStart int64, jm *JoinMetrics, f func(KeyIdScore) bool) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	start := time.Now()
	exit := false
	ms := v.majorScore()

	iend, cmp, step := int64(-1), 1, int64(-1)
	if !desc {
		iend, cmp, step = int64(len(b.keys)), -1, 1
	}

NEXT:
	for i := end1; icmp(i, iend) == cmp; i += step {
		if !fast.contains(uint16((hr*slotSize + int(i)) / fastSlotSize)) {
			continue
		}
		jm.Slots[hr].Scans++
		xf, vs := xfBuild(b.xfs[b.prevSpan(i):b.spans[i]])

		oneof := true
		for _, hs := range v.Oneof {
			if oneof = xfContains(xf, vs, hs); oneof {
				break
			}
		}
		if !oneof {
			continue
		}

		s := 0
		for _, hs := range v.Major {
			if xfContains(xf, vs, hs) {
				s++
			}
		}
		if s < ms {
			continue
		}

		for _, hs := range v.Exact {
			if !xfContains(xf, vs, hs) {
				continue NEXT
			}
		}

		jm.Slots[hr].Hits++
		if !f(KeyIdScore{
			Key:   b.keys[i],
			Id:    int64(hr*slotSize) + int64(i) + baseStart,
			Score: s,
		}) {
			exit = true
			break
		}
	}

	jm.Slots[hr].Elapsed = time.Since(start)
	return exit
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

	for i := range b.slots {
		b.slots[i], err = readSubMap(rd)
		if err != nil {
			return nil, err
		}
	}

	verify := h.Sum32()
	var checksum uint32
	if err := binary.Read(rd, binary.BigEndian, &checksum); err != nil {
		return nil, fmt.Errorf("read checksum: %v", err)
	}
	if checksum != verify {
		return nil, fmt.Errorf("invalid header checksum %x and %x", verify, checksum)
	}
	if err != nil {
		return nil, fmt.Errorf("read header: %v", err)
	}

	return b, nil
}

func readSubMap(rd io.Reader) (*subMap, error) {
	b := &subMap{}

	var keysLen uint32
	if err := binary.Read(rd, binary.BigEndian, &keysLen); err != nil {
		return nil, fmt.Errorf("read keys length: %v", err)
	}

	tmp := make([]byte, keysLen*uint32(KeySize))
	if err := binary.Read(rd, binary.BigEndian, tmp); err != nil {
		return nil, fmt.Errorf("read keys: %v", err)
	}
	b.keys = bytesKeys(tmp)

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

	return b, nil
}

func (b *Range) MarshalBinary(compress bool) []byte {
	p := &bytes.Buffer{}
	b.Marshal(p, compress)
	return p.Bytes()
}

func (b *Range) Marshal(w io.Writer, compress bool) (int, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	mw := &meterWriter{Writer: w}

	var zw io.WriteCloser
	if compress {
		mw.Write([]byte{4})
		zw = lz4.NewWriter(mw)
	} else {
		mw.Write([]byte{1})
		zw = mw
	}

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
	for _, h := range b.slots {
		if err := h.writeTo(w); err != nil {
			return 0, err
		}
	}
	// Write CRC32 checksum to the end of stream.
	if err := binary.Write(w, binary.BigEndian, h.Sum32()); err != nil {
		return 0, err
	}
	if err := zw.Close(); err != nil {
		return 0, err
	}
	return mw.size, nil
}

func (b *subMap) writeTo(w io.Writer) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := binary.Write(w, binary.BigEndian, uint32(len(b.keys))); err != nil {
		return err
	}
	if _, err := w.Write(keysBytes(b.keys)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, b.spans); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, b.xfs); err != nil {
		return err
	}
	return nil
}

func (b *Range) RoughSizeBytes() (sz int64) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	sz += int64(b.fastTable.GetSerializedSizeInBytes())
	for i := range b.slots {
		sz += int64(len(b.slots[i].xfs))
		sz += int64(len(b.slots[i].keys)) * (int64(KeySize) + 4)
	}
	return
}

func (b *Range) String() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	start := time.Now()
	buf := &bytes.Buffer{}

	m := roaring.New()
	b.fastTable.Iterate(func(x uint32) bool {
		m.Add(x & fastSlotMask)
		return true
	})

	fmt.Fprintf(buf, "range: %d-%d, count: %d, rough size: %db\n", b.start, b.End(), b.Len(), b.RoughSizeBytes())
	fmt.Fprintf(buf, "fast table: num=%d, size=%db, #hash=%d, bf=%d\n",
		b.fastTable.GetCardinality(), b.fastTable.GetSerializedSizeInBytes(), m.GetCardinality(), b.hashNum)
	for i, h := range b.slots {
		h.debug(i, buf)
	}

	fmt.Fprintf(buf, "collected in %v", time.Since(start))
	return buf.String()
}

func (b *subMap) debug(i int, buf io.Writer) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.keys) > 0 {
		fmt.Fprintf(buf, "[%02d;0x%05x] ", i, i*slotSize)
		fmt.Fprintf(buf, "keys: %5d/%2d, last key: %v, filter size: %db\n",
			len(b.keys), len(b.keys)/fastSlotSize, b.keys[len(b.keys)-1], len(b.xfs))
	}
}

func (b *Range) joinFast(vs *Values) (res bitmap1024) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	type hashState struct {
		h uint32
		bitmap1024
	}

	hashStates := map[uint32]*hashState{}
	fill := func(hashes []uint64, mask uint32) [][4]uint32 {
		var out [][4]uint32
		for _, v := range hashes {
			h := h16(uint32(v), b.start)
			for i := 0; i < int(b.hashNum); i++ {
				hashStates[h[i]] = &hashState{h: h[i] & mask}
			}
			out = append(out, h)
		}
		return out
	}
	oneofHashes := fill(vs.Oneof, fastSlotMask)
	majorHashes := fill(vs.Major, fastSlotMask)
	exactHashes := append(fill(vs.Exact, fastSlotMask), fill(vs.HighEntropy, highEntropyMask)...)

	iter := b.fastTable.Iterator().(*roaring.IntIterator)
	for _, hs := range hashStates {
		iter.Seek(hs.h)
		for iter.HasNext() {
			h2 := iter.Next()
			if h2&fastSlotMask == hs.h {
				hs.add(uint16(h2 - hs.h))
			} else {
				break
			}
		}
	}

	// z := time.Now()
	var final *bitmap1024
	for _, raw := range oneofHashes {
		m := hashStates[raw[0]].bitmap1024
		for i := 1; i < int(b.hashNum); i++ {
			m.and(&hashStates[raw[i]].bitmap1024)
		}
		if final == nil {
			final = &m
		} else {
			final.or(&m)
		}
	}

	var major *bitmap1024
	var scores map[uint16]int
	for _, raw := range majorHashes {
		m := hashStates[raw[0]].bitmap1024
		for i := 1; i < int(b.hashNum); i++ {
			m.and(&hashStates[raw[i]].bitmap1024)
		}
		if major == nil {
			major = &m
			scores = map[uint16]int{}
		} else {
			major.or(&m)
		}
		m.iterate(func(x uint16) bool { scores[x]++; return true })
	}
	if major != nil {
		ms := vs.majorScore()
		var res bitmap1024
		major.iterate(func(offset uint16) bool {
			if scores[offset] >= ms {
				res.add(offset)
			}
			return true
		})
		if final == nil {
			final = &res
		} else {
			final.and(&res)
		}
	}

	for _, raw := range exactHashes {
		m := hashStates[raw[0]].bitmap1024
		for i := 1; i < int(b.hashNum); i++ {
			m.and(&hashStates[raw[i]].bitmap1024)
		}
		if final == nil {
			final = &m
		} else {
			final.and(&m)
		}
	}

	if final == nil {
		return bitmap1024{}
	}
	return *final
}

func (b *Range) Find(key Key) (int64, func(uint64) bool) {
	for hr, m := range b.slots {
		m.mu.Lock()
		for i, k := range m.keys {
			if k == key {
				x, vs := xfBuild(m.xfs[m.prevSpan(int64(i)):m.spans[i]])
				m.mu.Unlock()
				return int64(hr)*slotSize + int64(i), func(k uint64) bool { return xfContains(x, vs, k) }
			}
		}
		m.mu.Unlock()
	}
	return 0, nil
}
