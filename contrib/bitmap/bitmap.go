package bitmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

const BitmapSeconds = 3600

type Bitmap struct {
	mu       sync.RWMutex
	top      *roaring.Bitmap
	children []*roaring.Bitmap
	ts       []uint32
	baseTime int64
}

func New(baseTime int64) *Bitmap {
	return &Bitmap{
		top:      roaring.New(),
		baseTime: baseTime,
	}
}

func (b *Bitmap) Add(ts int64, v uint32) bool {
	if ts-b.baseTime > BitmapSeconds {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	offset := uint32(ts - b.baseTime)
	b.top.Add(offset)
	idx := sort.Search(len(b.ts), func(i int) bool { return b.ts[i] >= offset })
	if idx < len(b.ts) && b.ts[idx] == offset {
		return b.children[idx].CheckedAdd(v)
	}
	tmp := roaring.New()
	b.children = append(b.children[:idx], append([]*roaring.Bitmap{tmp}, b.children[idx:]...)...)
	b.ts = append(b.ts[:idx], append([]uint32{offset}, b.ts[idx:]...)...)
	return tmp.CheckedAdd(v)
}

func (b *Bitmap) MergeTimestamps(vs []uint32) (res []int64) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	iter := b.top.Iterator()
	for i := 0; iter.HasNext(); i++ {
		v := iter.Next()
		for _, x := range vs {
			if b.children[i].Contains(x) {
				ts := b.baseTime + int64(v)
				res = append(res, ts)
				break
			}
		}
	}
	return
}

func FromBinary(buf []byte) (*Bitmap, error) {
	rd := bytes.NewReader(buf)
	b := &Bitmap{}
	if err := binary.Read(rd, binary.BigEndian, &b.baseTime); err != nil {
		return nil, fmt.Errorf("read baseTime: %v", err)
	}

	var topSize uint64
	if err := binary.Read(rd, binary.BigEndian, &topSize); err != nil {
		return nil, fmt.Errorf("read topSize: %v", err)
	}

	b.top = roaring.New()
	if _, err := b.top.ReadFrom(io.LimitReader(rd, int64(topSize))); err != nil {
		return nil, fmt.Errorf("read top: %v", err)
	}

	iter := b.top.Iterator()
	for iter.HasNext() {
		v := iter.Next()
		var childSize uint64
		if err := binary.Read(rd, binary.BigEndian, &childSize); err != nil {
			return nil, fmt.Errorf("read childSize: %v", err)
		}

		tmp := roaring.New()
		if _, err := tmp.ReadFrom(io.LimitReader(rd, int64(childSize))); err != nil {
			return nil, fmt.Errorf("read child: %v", err)
		}

		b.ts = append(b.ts, v)
		b.children = append(b.children, tmp)
	}
	return b, nil
}

func (b *Bitmap) MarshalBinary() []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	buf := &bytes.Buffer{}

	binary.Write(buf, binary.BigEndian, b.baseTime)
	binary.Write(buf, binary.BigEndian, b.top.GetSerializedSizeInBytes())
	b.top.WriteTo(buf)
	iter := b.top.Iterator()
	for i := 0; iter.HasNext(); i++ {
		v := iter.Next()
		if v != b.ts[i] {
			panic("unmatched timestamp")
		}
		m := b.children[i]
		binary.Write(buf, binary.BigEndian, m.GetSerializedSizeInBytes())
		m.WriteTo(buf)
	}

	return buf.Bytes()
}
