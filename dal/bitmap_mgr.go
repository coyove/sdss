package dal

import (
	"fmt"
	"math/bits"
	"strconv"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/coyove/sdss/contrib/clock"
)

const (
	bitmapTimeSpan = 10 // 86400 * 10 // 10 days
)

type bitmap struct {
	*roaring.Bitmap
	ts   int64 // rounded to bitmapTimeSpan
	name string
}

var bm struct {
	mu sync.Mutex
	m  map[string]*bitmap
}

func init() {
	// os.MkdirAll("token_test", 0777)
}

func addBitmap(ns, name, id string) error {
	idUnix, ok := clock.ParseStrUnix(id)
	if !ok {
		return fmt.Errorf("bitmap add %q: invalid timestamp format", id)
	}
	normalizedUnix := idUnix / bitmapTimeSpan * bitmapTimeSpan
	name = genBitmapBlockName(ns, name, normalizedUnix, "")

	bm.mu.Lock()
	if bm.m == nil {
		bm.m = map[string]*bitmap{}
	}
	m, ok := bm.m[name]
	if !ok {
		m = &bitmap{
			Bitmap: roaring.NewBitmap(),
			ts:     normalizedUnix,
			name:   name,
		}
		bm.m[name] = m
	} else {
		m2 := *m
		m2.Bitmap = m.Clone()
		m = &m2
	}
	bm.mu.Unlock()

	if idUnix < m.ts || idUnix >= m.ts+bitmapTimeSpan {
		return fmt.Errorf("bitmap add %q: fatal clock (currently %d), got ID at %d", id, m.ts, idUnix)
	}

	diff := idUnix - m.ts
	m.CheckedAdd(uint32(diff))

	bm.mu.Lock()
	bm.m[name] = m
	bm.mu.Unlock()
	return nil
}

func visitBitmap(key string, f func(*bitmap)) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	f(bm.m[key])
}

func mergeBitmaps(ns string, includes, excludes []string, start, end int64, f func(int64) bool) error {
	rawStart := start
	start = start / bitmapTimeSpan * bitmapTimeSpan
	end = end / bitmapTimeSpan * bitmapTimeSpan
	if start < end {
		return nil
	}

	var final *roaring.Bitmap
	var fmu sync.Mutex
	var wg sync.WaitGroup
	for _, name := range includes {
		wg.Add(1)
		go visitBitmap(genBitmapBlockName(ns, name, start, ""), func(b *bitmap) {
			defer wg.Done()
			if b == nil {
				return
			}
			fmu.Lock()
			if final == nil {
				final = b.Clone()
			} else {
				final.Or(b.Bitmap)
			}
			fmu.Unlock()
		})
	}
	wg.Wait()

	if final == nil {
		return mergeBitmaps(ns, includes, excludes, start-bitmapTimeSpan, end, f)
	}

	for _, name := range excludes {
		visitBitmap(genBitmapBlockName(ns, name, start, ""), func(b *bitmap) {
			if b == nil {
				return
			}
			final.AndNot(b.Bitmap)
		})
	}

	iter := final.ReverseIterator()
	for iter.HasNext() {
		ts := int64(iter.Next()) + start
		if ts > rawStart {
			continue
		}
		if !f(ts) {
			return nil
		}
	}
	return mergeBitmaps(ns, includes, excludes, start-bitmapTimeSpan, end, f)
}

func genBitmapBlockName(ns, name string, unix int64, serverId string) string {
	if len(ns) > 255 || len(name) > 255 {
		panic("namespace or name overflows")
	}
	head := uint16(len(ns))<<8 | uint16(len(name))
	return fmt.Sprintf("%04x%s%s%016x%s", bits.Reverse16(head), ns, name, unix, serverId)
}

func parseBitmapBlockName(s string) (ns, name string, unix int64, serverId string, ok bool) {
	if len(s) < 4 {
		return
	}
	tmp, err := strconv.ParseUint(s[:4], 16, 64)
	if err != nil {
		return
	}
	s = s[4:]
	x := bits.Reverse16(uint16(tmp))
	nsLen, nameLen := int(x>>8), int(byte(x))

	if len(s) < nsLen+nameLen {
		return
	}
	ns, name = s[:nsLen], s[nsLen:nsLen+nameLen]
	s = s[nsLen+nameLen:]

	if len(s) == 16 { // no server_id suffix
		unix, err = strconv.ParseInt(s, 16, 64)
		if err != nil {
			return
		}
	} else if len(s) > 16 {
		unix, err = strconv.ParseInt(s[:16], 16, 64)
		if err != nil {
			return
		}
		serverId = s[16:]
	} else {
		return
	}
	ok = true
	return
}

func implGetBitmap(key string) (*bitmap, error) {
	return nil, nil
}
