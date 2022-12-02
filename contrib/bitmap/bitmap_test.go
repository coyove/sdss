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
			res = append(res, strings.TrimSpace(line))
			ln = ln[1:]
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
		hs := []uint32{}
		for k := range ngram.Split(string(line)) {
			hs = append(hs, types.StrHash(k))
		}
		b.AddWithTime(uint64(i), now*10+int64(tso), hs)

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

	gs := ngram.Split("chinese noodle")
	if false {
		gs = ngram.Split(`Scalloped Corn,"[""1 can cream-style corn"", ""1 can whole kernel corn"", ""1/2 pkg. (approximately 20) saltine crackers, crushed"", ""1 egg, beaten"", ""6 tsp. butter, divided"", ""pepper to taste""]","[""Mix
	 together both cans of corn, crackers, egg, 2 teaspoons of melted butter and pepper and place in a buttered baking dish."", ""Dot with remaining 4 teaspoons of butter."", ""Bake at 350 for 1 hour.""]",www.
	cookbooks.com/Recipe-Details.aspx?id=876969,Gathered,"[""cream-style corn"", ""whole kernel corn"", ""crackers"", ""egg"", ""butter"", ""pepper""]"
	8,Nolan'S Pepper Steak,"[""1 1/2 lb. round steak (1-inch thick), cut into strips"", ""1 can drained tomatoes, cut up (save liquid)"", ""1 3/4 c. water"", ""1/2 c. onions"", ""1 1/2 Tbsp. Worcestershire sauce"",
	""2 green peppers, diced"", ""1/4 c. oil""]","[""Roll steak strips in flour."", ""Brown in skillet."", ""Salt and pepper."", ""Combine tomato liquid, water, onions and browned steak. Cover and simmer for one and
	 a quarter hours."", ""Uncover and stir in Worcestershire sauce."", ""Add tomatoes, green peppers and simmer for 5 minutes."", ""Serve over hot cooked rice.""]",www.cookbooks.com/Recipe-Details.aspx?id=375254,Ga
	thered,"[""tomatoes"", ""water"", ""onions"", ""Worcestershire sauce"", ""green peppers"", ""oil""]"`)
	}
	var q []uint32
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
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// results = b.Join(q, 16695936054, 50, JoinMajor)
			results = b.Join(nil, q, b.EndTimeDeci(), 50, JoinAll)
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
			fmt.Println(results[i].Key, int(results[i].UnixDeci), s, line)
			_ = i
			hits++
		}
	}
	fmt.Println(time.Since(start), hits, len(lines))
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
			b.AddWithTime(uint64(t*100000+i), now*10+int64(t), []uint32{v})
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

	b, _ = Unmarshal(bytes.NewReader(x))
	fmt.Println(b, ctr)

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
