package dal

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/coyove/common/lru"
	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/types"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

const (
	bitmapTimeSpan = 10 // 86400 * 10 // 10 days
)

type bitmap struct {
	*roaring.Bitmap
	sync.RWMutex
	nsid  string
	ts    string
	dirty bool
}

var bm struct {
	hot    sync.Map
	cache  *lru.Cache
	loader singleflight.Group
}

func init() {
	bm.cache = lru.NewCache(1000)
	// hotBitmapsUpdater()
}

func hotBitmapsUpdater() {
	defer func() {
		if r := recover(); r != nil {
			logrus.Error("hotBitmapsUpdater fatal: ", r)
		}
		time.AfterFunc(time.Second*5, hotBitmapsUpdater)
	}()

	var pendings []*bitmap
	var toDeletes []string
	var total int
	var deletePivot = clock.UnixToIdStr(clock.Unix() - bitmapTimeSpan*2)

	bm.hot.Range(func(k, v interface{}) bool {
		b := v.(*bitmap)
		if b.ts < deletePivot && !b.dirty {
			toDeletes = append(toDeletes, k.(string))
		} else if b.dirty {
			pendings = append(pendings, b)
		}
		total++
		return true
	})

	logrus.Infof("hotBitmapsUpdater payloads: %d deletes, %d updates, %d total",
		len(toDeletes), len(pendings), total)

	for _, k := range toDeletes {
		bm.hot.Delete(k)
	}

	if len(pendings) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, b := range pendings {
		b.RLock()
		buf, _ := b.MarshalBinary()
		b.dirty = false
		b.RUnlock()

		wg.Add(1)
		go func(nsid, ts string) {
			defer wg.Done()
			if _, err := db.UpdateItem(&dynamodb.UpdateItemInput{
				TableName: &tableFTS,
				Key: map[string]*dynamodb.AttributeValue{
					"nsid": {S: aws.String(nsid)},
					"ts":   {S: aws.String(ts)},
				},
				UpdateExpression: aws.String("set #a = :value"),
				ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
					":value": {B: buf},
				},
				ExpressionAttributeNames: map[string]*string{
					"#a": aws.String("content"),
				},
			}); err != nil {
				logrus.Errorf("hotBitmapsUpdater store error, key: %s.%s: %v", nsid, ts, err)
			}
		}(b.nsid, b.ts)
	}
	wg.Wait()
}

// hash(ns + name) into 8 bits, 16 + 16
func addBitmap(ns, token, id string) error {
	idUnix, ok := clock.ParseStrUnix(id)
	if !ok {
		return fmt.Errorf("bitmap add %q: invalid timestamp format", id)
	}

	normalizedUnix := idUnix / bitmapTimeSpan * bitmapTimeSpan
	partKey := genBitmapPartKey(ns, token)
	unixStr := fmt.Sprintf("%016x", normalizedUnix)
	key := partKey + unixStr

	cached, ok := bm.hot.Load(key)
	if !ok {
		loaded, err, _ := bm.loader.Do(key, func() (interface{}, error) {
			v, err := dalGetBitmap(partKey, unixStr)
			if v == nil && err == nil {
				v = &bitmap{
					Bitmap: roaring.NewBitmap(),
					nsid:   partKey,
					ts:     unixStr,
				}
			}
			return v, err
		})
		if err != nil {
			return err
		}
		cached = loaded
		bm.hot.Store(key, loaded)
	}

	m, _ := cached.(*bitmap)
	diff := idUnix - normalizedUnix
	m.Lock()
	if m.CheckedAdd((types.StrHash(token)&0xffff)<<16 | uint32(diff)) {
		m.dirty = true
	}
	buf, _ := m.MarshalBinary()
	m.Unlock()

	if _, err := db.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: &tableFTS,
		Key: map[string]*dynamodb.AttributeValue{
			"nsid": {S: aws.String(partKey)},
			"ts":   {S: aws.String(unixStr)},
		},
		UpdateExpression: aws.String("set #a = :value"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":value": {B: buf},
		},
		ExpressionAttributeNames: map[string]*string{
			"#a": aws.String("content"),
		},
	}); err != nil {
		logrus.Errorf("hotBitmapsUpdater store error, key: %s.%s: %v", partKey, unixStr, err)
	}
	return nil //dalPutBitmap(partKey, unixStr, m)
}

func accessBitmapReadonly(partKey string, unix int64, f func(*bitmap)) error {
	unixStr := fmt.Sprintf("%016x", unix)
	cacheKey := partKey + unixStr
	cached, ok := bm.hot.Load(cacheKey)
	if !ok {
		cached, ok = bm.cache.Get(cacheKey)
	}
	if !ok {
		loaded, err, _ := bm.loader.Do(cacheKey, func() (interface{}, error) {
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
		bm.cache.Add(cacheKey, cached)
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
	var dict = roaring.New()

	var wg sync.WaitGroup
	for _, token := range includes {
		wg.Add(1)
		go accessBitmapReadonly(genBitmapPartKey(ns, token), start, func(b *bitmap) {
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
		dict.Add(types.StrHash(token) & 0xffff)
	}
	wg.Wait()

	if final == nil {
		// No hits in current time block (start),
		// so we will search for the nearest block among all tokens.
		out := make([]int, len(includes))
		for i, token := range includes {
			wg.Add(1)
			go func(i int, partKey string) {
				defer wg.Done()
				resp, _ := db.Query(&dynamodb.QueryInput{
					TableName:              &tableFTS,
					KeyConditionExpression: aws.String("nsid = :pk and #ts < :upper"),
					ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
						":pk":    {S: aws.String(partKey)},
						":upper": {S: aws.String(fmt.Sprintf("%016x", start))},
					},
					ExpressionAttributeNames: map[string]*string{
						"#ts": aws.String("ts"),
					},
					ScanIndexForward: aws.Bool(false),
					Limit:            aws.Int64(1),
				})
				if resp != nil && len(resp.Items) == 1 {
					ts, _ := strconv.ParseInt(strings.TrimLeft(*resp.Items[0]["ts"].S, "0"), 16, 64)
					out[i] = int(ts)
				}
			}(i, genBitmapPartKey(ns, token))
		}
		wg.Wait()
		sort.Ints(out)
		if last := out[len(out)-1]; last > 0 {
			return mergeBitmaps(ns, includes, excludes, int64(last)+bitmapTimeSpan-1, end, f)
		}
		return nil
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
	tsDedup := roaring.New()
	for iter.HasNext() {
		tmp := iter.Next()
		offset := tmp & 0xffff
		ts := int64(offset) + start
		if !dict.Contains(tmp >> 16) {
			continue
		}
		if ts > rawStart {
			continue
		}
		if tsDedup.Contains(offset) {
			continue
		}
		if !f(ts) {
			return nil
		}
		tsDedup.Add(offset)
	}
	return mergeBitmaps(ns, includes, excludes, start-1, end, f)
}

func genBitmapPartKey(ns, token string) string {
	return fmt.Sprintf("%s#%02x", ns, (types.StrHash(token)>>8)&0xff)
}

func dalGetBitmap(nsid, unix string) (*bitmap, error) {
	// v, ok := zzz.Load(nsid + unix)
	// if !ok {
	// 	return nil, nil
	// }
	// m := roaring.New()
	// if err := m.UnmarshalBinary(v.([]byte)); err != nil {
	// 	return nil, err
	// }
	// return &bitmap{
	// 	Bitmap: m,
	// 	key:    nsid,
	// }, nil
	resp, err := db.GetItem(&dynamodb.GetItemInput{
		TableName: &tableFTS,
		Key: map[string]*dynamodb.AttributeValue{
			"nsid": {S: aws.String(nsid)},
			"ts":   {S: aws.String(unix)},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("dal get bitmap: store error: %v", err)
	}

	v := resp.Item["content"]
	if v == nil || len(v.B) == 0 {
		return nil, nil
	}
	m := roaring.New()
	if err := m.UnmarshalBinary(v.B); err != nil {
		return nil, err
	}
	return &bitmap{
		Bitmap: m,
		nsid:   nsid,
		ts:     unix,
	}, nil
}
