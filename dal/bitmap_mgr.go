package dal

import (
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/coyove/common/lru"
	"github.com/coyove/sdss/contrib/clock"
	"golang.org/x/sync/singleflight"
)

const (
	bitmapTimeSpan = 10 // 86400 * 10 // 10 days
)

type bitmap struct {
	*roaring.Bitmap
	sync.RWMutex
	key string
}

var zzz sync.Map

var bm struct {
	m      *lru.Cache
	loader singleflight.Group
}

func init() {
	// os.MkdirAll("token_test", 0777)
	bm.m = lru.NewCache(1000)
}

// hash(ns + name) into 12 bits, 15 + 17
func addBitmap(ns, name, id string) error {
	idUnix, ok := clock.ParseStrUnix(id)
	if !ok {
		return fmt.Errorf("bitmap add %q: invalid timestamp format", id)
	}

	normalizedUnix := idUnix / bitmapTimeSpan * bitmapTimeSpan
	partKey := genBitmapPartKey(ns, name)
	unixStr := fmt.Sprintf("%016x", normalizedUnix)
	key := partKey + unixStr

	cached, ok := bm.m.Get(key)
	if !ok {
		loaded, err, _ := bm.loader.Do(key, func() (interface{}, error) {
			v, err := dalGetBitmap(partKey, unixStr)
			if v == nil && err == nil {
				v = &bitmap{
					Bitmap: roaring.NewBitmap(),
					key:    key,
				}
			}
			return v, err
		})
		if err != nil {
			return err
		}
		cached = loaded
		bm.m.Add(key, loaded)
	}

	m, _ := cached.(*bitmap)
	diff := idUnix - normalizedUnix
	m.Lock()
	m.CheckedAdd((strhash(name)&0xffff)<<16 | uint32(diff))
	m.Unlock()

	return dalPutBitmap(partKey, unixStr, m)
}

func accessBitmapReadonly(partKey string, unix int64, f func(*bitmap)) error {
	unixStr := fmt.Sprintf("%016x", unix)
	key := partKey + unixStr
	cached, ok := bm.m.Get(key)
	if !ok {
		loaded, err, _ := bm.loader.Do(key, func() (interface{}, error) {
			v, err := dalGetBitmap(partKey, unixStr)
			if v == nil { // avoid nil to nil interface{}
				return nil, err
			}
			return v, err
		})
		if err != nil {
			return err
		}
		if loaded == nil {
			f(nil)
			return nil
		}
		cached, _ = loaded.(*bitmap)
		bm.m.Add(key, cached)
	}
	b := cached.(*bitmap)
	b.RLock()
	defer b.RUnlock()
	f(b)
	return nil
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
	var dict = roaring.New()
	for _, name := range includes {
		wg.Add(1)
		go accessBitmapReadonly(genBitmapPartKey(ns, name), start, func(b *bitmap) {
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
		dict.Add(strhash(name) & 0xffff)
	}
	wg.Wait()

	if final == nil {
		return mergeBitmaps(ns, includes, excludes, start-bitmapTimeSpan, end, f)
	}

	for _, name := range excludes {
		accessBitmapReadonly(genBitmapPartKey(ns, name), start, func(b *bitmap) {
			if b == nil {
				return
			}
			final.AndNot(b.Bitmap)
		})
	}

	iter := final.ReverseIterator()
	for iter.HasNext() {
		tmp := iter.Next()
		if !dict.Contains(tmp >> 16) {
			continue
		}
		ts := int64(tmp&0xffff) + start
		if ts > rawStart {
			continue
		}
		if !f(ts) {
			return nil
		}
	}
	return mergeBitmaps(ns, includes, excludes, start-bitmapTimeSpan, end, f)
}

func genBitmapPartKey(ns, name string) string {
	return ns + hash12(name)
}

func dalGetBitmap(nsid, unix string) (*bitmap, error) {
	v, ok := zzz.Load(nsid + unix)
	if !ok {
		return nil, nil
	}
	m := roaring.New()
	if err := m.UnmarshalBinary(v.([]byte)); err != nil {
		return nil, err
	}
	return &bitmap{
		Bitmap: m,
		key:    nsid,
	}, nil
	// resp, err := db.GetItem(&dynamodb.GetItemInput{
	// 	TableName: &tableFTS,
	// 	Key: map[string]*dynamodb.AttributeValue{
	// 		"nsid": {S: aws.String(nsid)},
	// 		"ts":   {S: aws.String(unix)},
	// 	},
	// })
	// if err != nil {
	// 	return nil, fmt.Errorf("dal get bitmap: store error: %v", err)
	// }

	// v := resp.Item["content"]
	// if v == nil || len(v.B) == 0 {
	// 	return nil, nil
	// }
	// m := roaring.New()
	// if err := m.UnmarshalBinary(v.B); err != nil {
	// 	return nil, err
	// }
	// return &bitmap{
	// 	Bitmap: m,
	// 	key:    nsid,
	// }, nil
}

func dalPutBitmap(nsid string, unix string, b *bitmap) error {
	b.RLock()
	buf, err := b.MarshalBinary()
	b.RUnlock()
	if err != nil {
		return err
	}
	zzz.Store(nsid+unix, buf)
	return nil

	if _, err := db.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: &tableFTS,
		Key: map[string]*dynamodb.AttributeValue{
			"nsid": {S: aws.String(nsid)},
			"ts":   {S: aws.String(unix)},
		},
		UpdateExpression: aws.String("set #a = :value"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":value": {B: buf},
		},
		ExpressionAttributeNames: map[string]*string{
			"#a": aws.String("content"),
		},
	}); err != nil {
		return fmt.Errorf("dal put bitmap: store error: %v", err)
	}
	return nil
}

func strhash(s string) uint32 {
	const offset32 = 2166136261
	const prime32 = 16777619
	var hash uint32 = offset32
	for i := range s {
		hash *= prime32
		hash ^= uint32(s[i])
	}
	return hash
}

func hash12(s string) string { return fmt.Sprintf("%03x", strhash(s)&0xfff) }
