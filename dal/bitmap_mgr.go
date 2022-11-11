package dal

import (
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/coyove/sdss/contrib/clock"
)

const (
	bitmapTimeSpan = 45 * 86400 * 1000 // 45 days in milliseconds
)

type bitmap struct {
	m     *roaring.Bitmap
	dirty bool
	ts    int64 // rounded to bitmapTimeSpan
}

var bm struct {
	mu sync.Mutex
	m  map[string]*bitmap
}

func AddBitmap(name, id string) error {
	idUnix, ok := clock.ParseStrUnixMilli(id)
	if !ok {
		return fmt.Errorf("bitmap add %q: invalid timestamp format", id)
	}
	normalizedUnix := idUnix / bitmapTimeSpan
	name = fmt.Sprintf("%s:%04x:%04x", name, normalizedUnix, clock.ServerId())

	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.m == nil {
		bm.m = map[string]*bitmap{}
	}

	m, ok := bm.m[name]
	if !ok {
		m = &bitmap{
			m:  roaring.NewBitmap(),
			ts: normalizedUnix,
		}
		bm.m[name] = m
	}

	if idUnix < m.ts || idUnix >= m.ts+normalizedUnix {
		return fmt.Errorf("bitmap add %q: fatal clock (currently %d), got ID at %d", id, m.ts, idUnix)
	}

	diff := idUnix - m.ts
	if m.m.CheckedAdd(uint32(diff)) {
		m.dirty = true
	}
	return nil
}
