package bitmap

import (
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/contrib/ngram"
	"github.com/coyove/sdss/types"
)

func TestBitmap2(t *testing.T) {
	now := clock.Unix() / halfday * halfday
	rand.Seed(now)
	b := New(now)

	cached, _ := ioutil.ReadFile("cache")
	if len(cached) > 0 {
		b, _ = UnmarshalBinary(cached)
	}
	fmt.Println(b)

	f, _ := os.Open(os.Getenv("HOME") + "/dataset/dataset/full_dataset.csv")
	defer f.Close()

	rd := csv.NewReader(f)
	for i := 0; false && i <= 2000000; i++ {
		records, err := rd.Read()
		if err != nil {
			break
		}

		if i < 1e6 {
			continue
		}

		line := strings.Join(records, " ")
		for k := range ngram.Split(string(line)) {
			h := types.StrHash(k)
			b.Add(clock.Unix(), uint8(i), h)
		}

		if i%1000 == 0 {
			log.Println(i)
			time.Sleep(time.Millisecond * 200)
		}
	}
	x := b.MarshalBinary(clock.Unix())
	fmt.Println(len(x), b)

	ioutil.WriteFile("cache", x, 0777)

	start := clock.Now()
	var q []uint32
	for k := range ngram.Split("breadcrumb") {
		q = append(q, types.StrHash(k))
	}
	b.Join(q).Iterate(func(ts int64, tag, s int) bool {
		if int(s) == len(q) {
			// fmt.Println(ts, tag)
		}
		return true
	})
	fmt.Println(time.Since(start))
}

func TestBitmap(t *testing.T) {
	now := clock.Unix() / halfday * halfday
	rand.Seed(now)
	b := New(now)

	var xx []uint32
	var tmp []uint32
	for t := 0; t < halfday; t++ {
		N := 1000
		big := false
		if rand.Intn(5) == 0 {
			N = 10000
			big = true
		}
		for i := 0; i < N; i++ {
			v := rand.Uint32()
			b.Add(now+int64(t), uint8(i), v)
			xx = append(xx, v)
			if big {
				tmp = append(tmp, v)
			}
		}
	}

	start := clock.Now()
	x := b.MarshalBinary(now + 10000)
	fmt.Println(len(x), time.Since(start), b)

	b, _ = UnmarshalBinary(x)

	for t := 10000; t < 15000; t++ {
		for i := 0; i < 10000; i++ {
			v := rand.Uint32()
			b.Add(now+int64(t), uint8(i), v)
		}
	}

	start = clock.Now()
	x = b.MarshalBinary(now + 20000)
	fmt.Println(len(x), time.Since(start), b)

	start = clock.Now()
	x = b.MarshalBinary(now + halfday)
	fmt.Println(len(x), time.Since(start), b)

	b, _ = UnmarshalBinary(x)
}
