package bitmap

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/contrib/ngram"
	"github.com/coyove/sdss/types"
)

func filterKTS(v []KeyTimeScore, minScore int) []KeyTimeScore {
	for i := len(v) - 1; i >= 0; i-- {
		if v[i].Score < minScore {
			v = append(v[:i], v[i+1:]...)
		}
	}
	return v
}

func lineOf(path string, ln int) string {
	f, _ := os.Open(path)
	defer f.Close()
	rd := bufio.NewReader(f)
	for i := 0; ; i++ {
		line, err := rd.ReadString('\n')
		if err != nil {
			break
		}
		if i == ln {
			return strings.TrimSpace(line)
		}
	}
	return ""
}

func TestBitmap2(t *testing.T) {
	now := clock.Unix() / day * day
	rand.Seed(now)

	b := New(now, 2)

	cached, err := ioutil.ReadFile("cache")
	if len(cached) > 0 {
		b, err = UnmarshalBinary(cached)
	}
	fmt.Println(b, err)

	path := os.Getenv("HOME") + "/dataset/dataset/full_dataset.csv"
	f, _ := os.Open(path)
	defer f.Close()

	go func() {
		for {
			x := b.MarshalBinary()
			ioutil.WriteFile("cache", x, 0777)
			time.Sleep(time.Second * 10)
		}
	}()

	rd := csv.NewReader(f)
	tso := 0
	for i := 0; false && i < 2000000; i++ {
		records, err := rd.Read()
		if err != nil {
			break
		}

		// if i < 100000 {
		// 	continue
		// }

		line := strings.Join(records, " ")
		hs := []uint32{}
		for k := range ngram.Split(string(line)) {
			hs = append(hs, types.StrHash(k))
		}
		b.addWithTime(uint64(i), now*10+int64(tso), hs)

		if i%1000 == 0 {
			log.Println(i, b)
		}
		if rand.Intn(3) == 0 {
			tso++
		}
		// time.Sleep(time.Millisecond * 10)
	}

	x := b.MarshalBinary()
	fmt.Println(len(x), b)
	ioutil.WriteFile("cache", x, 0777)

	start := clock.Now()
	gs := ngram.Split("chinese")
	var q []uint32
	for k := range gs {
		q = append(q, types.StrHash(k))
	}

	results := b.Join(q, 0)
	fmt.Println(len(results))
	return
	hits, total := 0, map[int64]bool{}
	for _, res := range results {
		line := lineOf(path, int(res.Key))
		s := 0
		for _, v := range gs {
			if m, _ := regexp.MatchString("(?i)"+v.Raw, line); m {
				s++
			}
		}
		if s == len(gs) {
			fmt.Println(res, line)
			hits++
		}
		total[res.UnixDeci] = true
	}
	fmt.Println(time.Since(start), hits, len(total))
}

func TestBitmap(t *testing.T) {
	now := clock.Unix() / day * day
	rand.Seed(now)
	b := New(now, 2)

	ctr := 0
	var store []uint32
	for t := 0; t < 86400*10; t += rand.Intn(5) + 1 {
		N := 100
		if rand.Intn(150) == 0 {
			N = 2000
		}
		for i := 0; i < N; i++ {
			v := rand.Uint32()
			b.addWithTime(uint64(t*100000+i), now*10+int64(t), []uint32{v})
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

}
