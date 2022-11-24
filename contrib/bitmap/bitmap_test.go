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

	cached, _ := ioutil.ReadFile("cache")
	if len(cached) > 0 {
		b, _ = UnmarshalBinary(cached)
	}
	fmt.Println(b)

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
	for i := 0; false && i < 10000; i++ {
		records, err := rd.Read()
		if err != nil {
			break
		}

		if i < 0 {
			continue
		}

		line := strings.Join(records, " ")
		hs := []uint32{}
		for k := range ngram.Split(string(line)) {
			hs = append(hs, types.StrHash(k))
		}
		b.Add(uint64(i), hs)

		if i%100 == 0 {
			log.Println(i)
		}
		time.Sleep(time.Millisecond * 10)
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

	results := filterKTS(b.Join(q, 0, len(q)/2), len(q))
	hits, total := 0, map[int64]bool{}
	for _, res := range results {
		line := lineOf(path, int(res.Key))
		s := 0
		for k := range gs {
			if m, _ := regexp.MatchString("(?i)"+k, line); m {
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

	{
		var k uint32
		for k = range b.hours[0].hashIdx {
			break
		}
		fmt.Println(b.Join([]uint32{k}, 0, 0))
	}
}
