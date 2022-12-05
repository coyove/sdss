package bitmap

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/contrib/ngram"
	"github.com/coyove/sdss/types"
)

const day = 86400

func lineOf(path string, ln []int) (res []string) {
	sort.Ints(ln)

	f, _ := os.Open(path)
	defer f.Close()
	rd := bufio.NewReader(f)
	for i := 0; len(ln) > 0; i++ {
		line, err := rd.ReadString('\n')
		if err != nil {
			break
		}
		if i == ln[0] {
		AGAIN:
			res = append(res, strings.TrimSpace(line))
			ln = ln[1:]
			if len(ln) > 0 && ln[0] == i {
				goto AGAIN
			}
		}
	}
	return
}

func TestBitmap2(t *testing.T) {
	runtime.GOMAXPROCS(2)
	now := clock.Unix() / day * day
	rand.Seed(now)

	b := New(now, 2)

	cached, err := ioutil.ReadFile("cache")
	if len(cached) > 0 {
		b, err = Unmarshal(bytes.NewReader(cached))
	}
	fmt.Println(err)

	path := os.Getenv("HOME") + "/dataset/dataset/full_dataset.csv"
	f, _ := os.Open(path)
	defer f.Close()

	go func() {
		for {
			b.Save("cache")
			time.Sleep(time.Second * 10)
		}
	}()

	rd := csv.NewReader(f)
	tso := 0
	for i := 0; false && i < 100000; i++ {
		records, err := rd.Read()
		if err != nil {
			break
		}

		line := strings.Join(records, " ")
		hs := []uint64{}
		for k := range ngram.Split(string(line)) {
			hs = append(hs, types.StrHash(k))
		}
		b.Add(uint64(i), hs)

		if i%1000 == 0 {
			log.Println(i)
		}
		if rand.Intn(3) == 0 {
			tso++
		}
		// time.Sleep(time.Millisecond * 10)
	}

	x := b.MarshalBinary()
	fmt.Println(len(x), b)
	b.Save("cache")

	b.end = 1044
	_, zzz := ngram.SplitHash("lebih asik yg menantang.")
	fmt.Println(zzz)
	b.Add(1234567890, zzz)

	gs := ngram.Split("fuck")
	if false {
		gs = ngram.Split(`kernel corn"", ""1/2 pkg. (approximately 20) saltine crackers, crushed"", ""1 egg, beaten"", ""6 tsp. butter, divided"", ""pepper to taste""]","[""Mix
 together both cans of corn, crackers, egg, 2 teaspoons of melted butter and pepper and place in a buttered baking dish."", ""Dot with remaining 4 teaspoons of butter."", ""Bake at 350\u00b0 for 1 hour.""]",www.
cookbooks.com/Recipe-Details.aspx?id=876969,Gathered,"[""cream-style corn"", ""whole kernel corn"", ""crackers"", ""egg"", ""butter"", ""pepper""]" `)
	}
	var q []uint64
	for k := range gs {
		q = append(q, types.StrHash(k))
		fmt.Println(k, "==>", types.StrHash(k))
		if len(q) > 32 {
			break
		}
	}

	{
		f, _ := os.Create("cpuprofile")
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	start := time.Now()
	var results []KeyTimeScore
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results = b.Join(q, nil, 1670192109, 50, JoinMajor)
			// 	results = b.Join(q, nil, b.End(), 50, JoinMajor)
		}()
	}
	wg.Wait()
	fmt.Println(len(results), time.Since(start))
	hits := 0

	sort.Slice(results, func(i, j int) bool { return results[i].Key < results[j].Key })
	lineNums := []int{}
	for _, res := range results {
		lineNums = append(lineNums, int(res.Key))
	}
	lines := lineOf(path, lineNums)
	for i, line := range lines {
		s := 0
		for _, v := range gs {
			if m, _ := regexp.MatchString("(?i)"+v.Raw, line); m {
				s++
			}
		}
		if s >= len(gs)/2 {
			fmt.Println(results[i].Key, results[i].Id, s) // , line)
			_ = i
			hits++
		}
	}
	fmt.Println(time.Since(start), hits, len(lines))
}

func TestCollision(t *testing.T) {
	rand.Seed(clock.Unix())
	m := roaring.New()
	verify := map[uint32]*roaring.Bitmap{}
	x := rand.Perm(1440)
	for i := 0; i < 1e6; i++ {
		v := rand.Uint32()
		h := h16(v, 0)

		verify[v] = roaring.New()
		rand.Shuffle(len(x), func(i, j int) {
			x[i], x[j] = x[j], x[i]
		})
		for _, ts := range x[:rand.Intn(200)+200] {
			m.Add(h[0] + uint32(ts))
			m.Add(h[1] + uint32(ts))
			// m.Add(h[2] + uint32(ts))
			verify[v].Add(uint32(ts))
		}
		if i%1000 == 0 {
			fmt.Println(i)
		}
	}

	bad, total := 0, 0
	for v, ts := range verify {
		h := h16(v, 0)
		tmp := roaring.New()
		for i := 0; i < 1440; i++ {
			if m.Contains(h[0]+uint32(i)) && m.Contains(h[1]+uint32(i)) { // && m.Contains(h[2]+uint32(i)) {
				tmp.Add(uint32(i))
			}
		}
		tmp.AndNot(ts)
		total += int(ts.GetCardinality())
		bad += int(tmp.GetCardinality())
	}
	fmt.Println(bad, total)
}

func BenchmarkXor(b *testing.B) {
	var x []uint64
	var dedup = map[uint64]bool{}
	rand.Seed(clock.Unix())
	for i := 0; i < 1000; i++ {
		v := rand.Uint64()
		if dedup[v] {
			continue
		}
		x = append(x, v)
		dedup[v] = true
	}
	zzz := xfNew(x)
	fmt.Println(len(zzz))
	for i := 0; i < b.N; i++ {
		xfBuild(zzz)
	}
}
