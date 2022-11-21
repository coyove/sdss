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
	now := clock.UnixDeci() / day10 * day10
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

		if i < 1000000 {
			continue
		}

		line := strings.Join(records, " ")
		for k := range ngram.Split(string(line)) {
			h := types.StrHash(k)
			b.Add(clock.UnixDeci(), h)
		}

		if i%1000 == 0 {
			log.Println(i)
			time.Sleep(time.Millisecond * 200)
		}
	}

	x := b.MarshalBinary()
	fmt.Println(len(x), b)

	ioutil.WriteFile("cache", x, 0777)

	start := clock.Now()
	var q []uint32
	for k := range ngram.Split("function dictionary") {
		q = append(q, types.StrHash(k))
	}
	b.Join(q).Iterate(func(ts int64, s int) bool {
		if int(s) == len(q) {
			fmt.Println(ts)
		}
		return true
	})
	fmt.Println(time.Since(start))
}

func TestBitmap(t *testing.T) {
	now := clock.UnixDeci() / day10 * day10
	rand.Seed(now)
	b := New(now)

	ctr := 0
	var store []uint32
	for t := 0; t < day10; t += rand.Intn(5) + 1 {
		N := 100
		if rand.Intn(150) == 0 {
			N = 2000
		}
		for i := 0; i < N; i++ {
			v := rand.Uint32()
			b.Add(now+int64(t), v)
			ctr++
			store = append(store, v)
		}
		if t%2000 == 0 {
			fmt.Println(t)
		}
	}

	start := clock.Now()
	x := b.MarshalBinary()
	fmt.Println(len(x), time.Since(start))

	b, _ = UnmarshalBinary(x)
	fmt.Println(b, ctr)

	{
		var k uint32
		for k = range b.hours[0].hashIdx {
			break
		}
		b.Join([]uint32{k}).Iterate(func(ts int64, scores int) bool {
			// fmt.Println(ts, scores)
			return true
		})
	}
}
