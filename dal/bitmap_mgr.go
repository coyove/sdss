package dal

import (
	"fmt"
	"os"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/coyove/sdss/contrib/clock"
)

const (
	bitmapTimeSpan = 86400 * 1000 // 1 day in milliseconds
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

func init() {
	os.MkdirAll("ngram_test", 0777)
}

func addBitmap(ns, name, id string) error {
	idUnix, ok := clock.ParseStrUnixMilli(id)
	if !ok {
		return fmt.Errorf("bitmap add %q: invalid timestamp format", id)
	}
	normalizedUnix := idUnix / bitmapTimeSpan * bitmapTimeSpan
	name = genBitmapBlockName(ns, name, normalizedUnix)

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

	if idUnix < m.ts || idUnix >= m.ts+bitmapTimeSpan {
		return fmt.Errorf("bitmap add %q: fatal clock (currently %d), got ID at %d", id, m.ts, idUnix)
	}

	diff := idUnix - m.ts
	if m.m.CheckedAdd(uint32(diff)) {
		m.dirty = true
	}
	return nil
}

func visitBitmap(key string, f func(*bitmap)) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	f(bm.m[key])
}

func mergeBitmaps(ns string, includes, excludes []string, start int64, f func(ts int64) error) error {
	start = start / bitmapTimeSpan * bitmapTimeSpan

	var final *roaring.Bitmap
	for _, name := range includes {
		visitBitmap(genBitmapBlockName(ns, name, start), func(b *bitmap) {
			if b == nil {
				return
			}
			if final == nil {
				final = b.m.Clone()
			} else {
				final.Or(b.m)
			}
		})
	}

	if final == nil {
		return mergeBitmaps(ns, includes, excludes, start-bitmapTimeSpan, f)
	}

	for _, name := range excludes {
		visitBitmap(genBitmapBlockName(ns, name, start), func(b *bitmap) {
			if b == nil {
				return
			}
			final.AndNot(b.m)
		})
	}

	var err error
	final.Iterate(func(x uint32) bool {
		err = f(int64(x) + start)
		return err == nil
	})
	return err
}

func genBitmapBlockName(ns, name string, unixMilli int64) string {
	return fmt.Sprintf("%s:%s:%016x:%04x", ns, name, unixMilli, clock.ServerId())
}
