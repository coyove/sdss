package dal

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/coyove/common/lru"
	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/types"
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

// hash(ns + name) into 12 bits, 16 + 16
func addBitmap(ns, token, id string) error {
	idUnix, ok := clock.ParseStrUnix(id)
	if !ok {
		return fmt.Errorf("bitmap add %q: invalid timestamp format", id)
	}

	normalizedUnix := idUnix / bitmapTimeSpan * bitmapTimeSpan
	partKey := genBitmapPartKey(ns, token)
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
	m.CheckedAdd((types.StrHash(token)&0xffff)<<16 | uint32(diff))
	m.Unlock()

	return dalPutBitmap(partKey, unixStr, m)
}

func accessBitmapReadonly(partKey string, unix int64, f func(*bitmap)) error {
	unixStr := fmt.Sprintf("%016x", unix)
	cacheKey := partKey + unixStr
	cached, ok := bm.m.Get(cacheKey)
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
		bm.m.Add(cacheKey, cached)
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
	for iter.HasNext() {
		tmp := iter.Next()
		ts := int64(tmp&0xffff) + start
		if !dict.Contains(tmp >> 16) {
			continue
		}
		if ts > rawStart {
			continue
		}
		if !f(ts) {
			return nil
		}
	}
	return mergeBitmaps(ns, includes, excludes, start-bitmapTimeSpan, end, f)
}

func genBitmapPartKey(ns, token string) string {
	return fmt.Sprintf("%s#%03x", ns, types.StrHash(token)&0xfff)
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
		key:    nsid,
	}, nil
}

func dalPutBitmap(nsid string, unix string, b *bitmap) error {
	b.RLock()
	buf, err := b.MarshalBinary()
	b.RUnlock()
	if err != nil {
		return err
	}
	// zzz.Store(nsid+unix, buf)
	// return nil

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
