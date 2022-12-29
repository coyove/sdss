package dal

import (
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/coyove/common/lru"
	"github.com/coyove/sdss/contrib/bitmap"
	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/types"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/singleflight"
)

const (
	mergeBatchSize = 16
)

var bm struct {
	hot    sync.Map
	cache  *lru.Cache
	loader singleflight.Group
}

type NSBitmap struct {
	*bitmap.Range
	ns      string
	unixStr string
}

func init() {
	bm.cache = lru.NewCache(50)
	os.MkdirAll("bitmap_cache", 0777)
	hotBitmapsUpdater()
}

func hotBitmapsUpdater() {
	defer func() {
		if r := recover(); r != nil {
			logrus.Error("hotBitmapsUpdater fatal: ", r)
		}
		time.AfterFunc(time.Second*5, hotBitmapsUpdater)
	}()

	var pendings []*NSBitmap

	bm.hot.Range(func(k, v interface{}) bool {
		b := v.(*NSBitmap)
		pendings = append(pendings, b)
		return true
	})

	sz, fails := 0, 0
	for _, m := range pendings {
		x := m.MarshalBinary()
		if err := ioutil.WriteFile("bitmap_cache/"+m.ns+m.unixStr, x, 0777); err != nil {
			logrus.Errorf("hotBitmapsUpdater write cache %s %s: %v", m.ns, m.unixStr, err)
			fails++
		} else {
			sz += len(x)
		}
	}
	logrus.Infof("hotBitmapsUpdater payloads: %d total, %d fails, %d bytes",
		len(pendings), fails, sz)
}

func addBitmap(ns, id string, tokens map[string]float64) error {
	idUnix, ok := clock.ParseIdStrUnix(id)
	if !ok {
		return fmt.Errorf("bitmap add %q: invalid timestamp format", id)
	}

	day := idUnix / 86400 * 86400
	unixStr := fmt.Sprintf("%016x", day)
	key := ns + unixStr

	cached, ok := bm.hot.Load(key)
	if !ok {
		loaded, err, _ := bm.loader.Do(key, func() (interface{}, error) {
			v, err := dalGetBitmap(ns, unixStr)
			if v == nil && err == nil {
				v = &NSBitmap{
					Day:     bitmap.New(day),
					ns:      ns,
					unixStr: unixStr,
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

	m, _ := cached.(*NSBitmap)
	var hashes []uint32
	for tok := range tokens {
		hashes = append(hashes, types.StrHash(tok))
	}

	id64, _ := clock.Base40Decode(id)
	m.Add(id64, hashes)

	// if _, err := db.UpdateItem(&dynamodb.UpdateItemInput{
	// 	TableName: &tableFTS,
	// 	Key: map[string]*dynamodb.AttributeValue{
	// 		"nsid": {S: aws.String(partKey)},
	// 		"ts":   {S: aws.String(unixStr)},
	// 	},
	// 	UpdateExpression: aws.String("set #a = :value"),
	// 	ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
	// 		":value": {B: buf},
	// 	},
	// 	ExpressionAttributeNames: map[string]*string{
	// 		"#a": aws.String("content"),
	// 	},
	// }); err != nil {
	// 	logrus.Errorf("hotBitmapsUpdater store error, key: %s.%s: %v", partKey, unixStr, err)
	// }
	// zzz.Store(ns+unixStr, m)
	return nil
}

func accessBitmapReadonly(partKey string, unix int64, f func(*NSBitmap)) error {
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
		cached = loaded
		bm.cache.Add(cacheKey, cached)
	}
	f(cached.(*NSBitmap))
	return nil
}

func mergeBitmaps(ns string, includes []string, start, end int64, f func([]string) bool) error {
	rawStart := start
	start = start / 86400 * 86400
	end = end / 86400 * 86400
	if start < end {
		return nil
	}

	var hashes []uint32
	for _, token := range includes {
		hashes = append(hashes, types.StrHash(token))
	}

	var final []bitmap.KeyIdScore
	accessBitmapReadonly(ns, start, func(b *NSBitmap) {
		if b == nil {
			return
		}
		fmt.Println(b)
		final = b.Join(hashes, 0, true)
	})

	if len(final) == 0 {
		// No hits in current time block (start),
		// so we will search for the nearest block among all tokens.
		// out := make([]int, len(includes))
		// for i, token := range includes {
		// 	wg.Add(1)
		// 	go func(i int, partKey string) {
		// 		defer wg.Done()
		// 		resp, _ := db.Query(&dynamodb.QueryInput{
		// 			TableName:              &tableFTS,
		// 			KeyConditionExpression: aws.String("nsid = :pk and #ts < :upper"),
		// 			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
		// 				":pk":    {S: aws.String(partKey)},
		// 				":upper": {S: aws.String(fmt.Sprintf("%016x", start))},
		// 			},
		// 			ExpressionAttributeNames: map[string]*string{
		// 				"#ts": aws.String("ts"),
		// 			},
		// 			ScanIndexForward: aws.Bool(false),
		// 			Limit:            aws.Int64(1),
		// 		})
		// 		if resp != nil && len(resp.Items) == 1 {
		// 			ts, _ := strconv.ParseInt(strings.TrimLeft(*resp.Items[0]["ts"].S, "0"), 16, 64)
		// 			out[i] = int(ts)
		// 		}
		// 	}(i, genBitmapPartKey(ns, token))
		// }
		// wg.Wait()
		// sort.Ints(out)
		// if last := out[len(out)-1]; last > 0 {
		// 	return mergeBitmaps(ns, includes, excludes, int64(last)+bitmapTimeSpan-1, end, f)
		// }
		return nil
	}

	var ids []string
	for _, p := range final {
		if p.Score < len(hashes)/2 {
			continue
		}
		if p.UnixDeci > rawStart {
			continue
		}
		ids = append(ids, clock.Base40Encode(p.Key))
		if len(ids) >= mergeBatchSize {
			exit := f(ids)
			ids = ids[:0]
			if exit {
				break
			}
		}
	}
	if len(ids) >= 0 {
		if !f(ids) {
			return nil
		}
	}
	return mergeBitmaps(ns, includes, start-1, end, f)
}

func dalGetBitmap(ns, unixStr string) (*NSBitmap, error) {
	buf, err := ioutil.ReadFile("bitmap_cache/" + ns + unixStr)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	b, err := bitmap.UnmarshalBinary(buf)
	if err != nil {
		return nil, err
	}
	return &NSBitmap{
		Day:     b,
		ns:      ns,
		unixStr: unixStr,
	}, nil
	// v, ok := zzz.Load(ns + unixStr)
	// if !ok {
	// 	return nil, nil
	// }
	// return v.(*bitmap.Day), nil // bitmap.UnmarshalBinary(v.([]byte))
	// resp, err := db.GetItem(&dynamodb.GetItemInput{
	// 	TableName: &tableFTS,
	// 	Key: map[string]*dynamodb.AttributeValue{
	// 		"nsid": {S: aws.String(nsid)},
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
	// 	nsid:   nsid,
	// 	ts:     unix,
	// }, nil
}
