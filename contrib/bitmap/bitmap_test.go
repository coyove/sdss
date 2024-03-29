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

	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/contrib/ngram"
	"github.com/coyove/sdss/contrib/roaring"
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

	if false {
		rand.Seed(clock.Unix())
		m := roaring.New()
		m2 := roaring.New()
		ref := map[uint32][]uint32{}
		const N = 1e6
		const BF = 3
		for i := 0; i < N; i++ {
		AGAIN:
			x := rand.Uint32()
			if len(ref[x]) > 0 {
				goto AGAIN
			}
			h := h16(x, 0)
			for j := 0; j < 32; j += rand.Intn(2) + 1 {
				y := uint32(j)*128 + rand.Uint32()%128
				for bf := 0; bf < BF; bf++ {
					m.Add(h[bf]&0xfffff000 | (y & 0xfff))
					// m2.Add(h[bf]&0xfffffc00 | (y & 0x3ff))
				}
				ref[x] = append(ref[x], y)
			}
		}
		ys0, total, overflows, total2 := 0, 0, map[int]int{}, 0
		for x, ys := range ref {
			h := h16(x, 0)
			tmp := []*roaring.Bitmap{roaring.New(), roaring.New(), roaring.New(), roaring.New()}[:BF]
			for i := 0; i < BF; i++ {
				z := h[i] & 0xfffff000
				iter := m.Iterator().(*roaring.IntIterator)
				iter.Seek(z)
				for first := true; iter.HasNext(); first = false {
					v := iter.Next()
					if v&0xfffff000 == z {
						tmp[i].Add(v & 0xfff)
					} else {
						if first {
							panic(fmt.Sprintf("%x %x", v, z))
						}
						break
					}
				}
			}
			for i := 1; i < BF; i++ {
				tmp[0].And(tmp[i])
			}
			ys0 += len(ys)
			total += int(tmp[0].GetCardinality())
			for _, y := range ys {
				if !tmp[0].Contains(y) {
					fmt.Println(ys)
					panic(y)
				}
				tmp[0].Remove(y)
			}
			overflows[int(tmp[0].GetCardinality())]++
		}
		// for x := range ref {
		// 	h := h16(x, 0)
		// 	tmp := [2]*roaring.Bitmap{roaring.New(), roaring.New()}
		// 	for i := 0; i < 2; i++ {
		// 		z := h[i] & 0xfffffc00
		// 		iter := m2.Iterator().(*roaring.IntIterator)
		// 		iter.Seek(z)
		// 		for iter.HasNext() {
		// 			if v := iter.Next(); v&0xfffffc00 == z {
		// 				tmp[i].Add(v & 0x3ff)
		// 			} else {
		// 				break
		// 			}
		// 		}
		// 	}
		// 	tmp[0].And(tmp[1])
		// 	total2 += int(tmp[0].GetCardinality())
		// }
		fmt.Println(ys0, total, m.GetCardinality())
		fmt.Println(ys0, total2, m2.GetCardinality())

		a := make([]int, 10000)
		for k, n := range overflows {
			a[k] = n
		}
		tot := 0
		for i, a := range a {
			tot += a
			if tot >= int(N*0.99) {
				fmt.Println("p99 at", i)
				break
			}
		}
		return
	}

	b := New(now)
	cached, err := ioutil.ReadFile("cache")
	if len(cached) > 0 {
		b, err = Unmarshal(bytes.NewReader(cached))
	}
	fmt.Println(err)

	ba := b.AggregateSaves(func(b *Range) error {
		_, err := b.Save("cache", false)
		fmt.Println("save", err)
		return err
	})

	path := os.Getenv("HOME") + "/dataset/dataset/full_dataset.csv"
	f, _ := os.Open(path)
	defer f.Close()

	rd := csv.NewReader(f)
	for i := 0; true && i < 10000; i++ {
		records, err := rd.Read()
		if err != nil {
			break
		}

		line := strings.Join(records, " ")
		hs := []uint64{}
		for k := range ngram.Split(string(line)) {
			hs = append(hs, ngram.StrHash(k))
		}
		hs = append(hs, uint64(i))
		ba.AddAsync(Uint64Key(uint64(i)), hs)

		if i%1000 == 0 {
			log.Println(i)
		}
	}
	ba.Close()

	fmt.Println(len(b.MarshalBinary(true)), b)
	// b.Save("cache")

	gs := ngram.Split("chinese")
	if false {
		gs = ngram.Split(`kernel corn"", ""1/2 pkg. (approximately 20) saltine crackers, crushed"", ""1 egg, beaten"", ""6 tsp. butter, divided"", ""pepper to taste""]","[""Mix
 together both cans of corn, crackers, egg, 2 teaspoons of melted butter and pepper and place in a buttered baking dish."", ""Dot with remaining 4 teaspoons of butter."", ""Bake at 350\u00b0 for 1 hour.""]",www.
cookbooks.com/Recipe-Details.aspx?id=876969,Gathered,"[""cream-style corn"", ""whole kernel corn"", ""crackers"", ""egg"", ""butter"", ""pepper""]" `)
	}
	var q []uint64
	for k := range gs {
		q = append(q, ngram.StrHash(k))
		fmt.Println(k, "==>", ngram.StrHash(k))
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
	var results []KeyIdScore
	wg := sync.WaitGroup{}
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// results = b.Join(q, nil, 1670192109, 50, JoinMajor)
			var tmp []KeyIdScore
			fmt.Println(b.Join(Values{Major: q /* Oneof: []uint64{types.StrHash("cream")} */}, b.Start(), false, func(kis KeyIdScore) bool {
				tmp = append(tmp, kis)
				return len(tmp) < 50
			}))
			results = tmp
		}()
	}
	wg.Wait()
	fmt.Println((results), time.Since(start))
	hits := 0

	sort.Slice(results, func(i, j int) bool { return results[i].Key.Less(results[j].Key) })
	lineNums := []int{}
	for _, res := range results {
		lineNums = append(lineNums, int(res.Key.LowUint64()))
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
			fmt.Println(results[i].Key.LowUint64(), results[i].Id, s) // line)
			_ = i
			hits++
		}
	}
	fmt.Println(time.Since(start), hits, len(lines))
}

func TestCollision(t *testing.T) {
	tot := 0
	m2 := New(0)
	for i := 0; i < 1e6; i++ {
		m := roaring.New()
		var v []uint64
		for i := 0; i < 16; i++ {
			x := rand.Uint64()&0xfffff0 + uint64(i)
			m.Add(uint32(x))
			v = append(v, x)
		}
		m2.Add(Uint64Key(uint64(i)), v)
		tot += int(m.GetSerializedSizeInBytes())
	}
	fmt.Println(tot, len(m2.MarshalBinary(false)), m2.fastTable.GetSerializedSizeInBytes())
	return

	rand.Seed(clock.Unix())
	x := []uint64{}
	for i := 0; i < 1e3; i++ {
		v := rand.Uint64()
		x = append(x, v)
	}
	xf, vs := xfBuild(xfNew(x))
	for i := 0; ; i++ {
		if xfContains(xf, vs, rand.Uint64()) {
			panic(i)
		}
	}
}

func BenchmarkXorSmall(b *testing.B) {
	var x []uint64
	rand.Seed(clock.Unix())
	for i := 0; i < 5; i++ {
		v := rand.Uint64()
		x = append(x, v)
	}
	zzz := xfNew(x)
	fmt.Println(len(zzz))
	for i := 0; i < b.N; i++ {
		x, vs := xfBuild(zzz)
		if !xfContains(x, vs, uint64(vs[len(vs)-1])) {
			b.FailNow()
		}
	}
}

// func BenchmarkContainsBrute(b *testing.B) {
// 	var x []uint64
// 	rand.Seed(clock.Unix())
// 	for i := 0; i < 6; i++ {
// 		v := rand.Uint64()
// 		x = append(x, v)
// 	}
// 	n := rand.Uint64()
// 	for i := 0; i < b.N; i++ {
// 		for _, v0 := range x {
// 			if v0 == n {
// 				break
// 			}
// 		}
// 	}
// }
//
// func BenchmarkContainsXor(b *testing.B) {
// 	var x []uint64
// 	rand.Seed(clock.Unix())
// 	for i := 0; i < 6; i++ {
// 		v := rand.Uint64()
// 		x = append(x, v)
// 	}
// 	n := rand.Uint64()
// 	xf, _ := xorfilter.Populate(x)
// 	for i := 0; i < b.N; i++ {
// 		if xf.Contains(n) {
// 			break
// 		}
// 	}
// }
//
// func BenchmarkContainsBinary(b *testing.B) {
// 	var x []int
// 	rand.Seed(clock.Unix())
// 	for i := 0; i < 6; i++ {
// 		v := rand.Uint64()
// 		x = append(x, int(v))
// 	}
// 	n := int(rand.Uint64())
// 	for i := 0; i < b.N; i++ {
// 		sort.SearchInts(x, n)
// 	}
// }

func TestManager(t *testing.T) {
	m, _ := NewManager("mgr", 10, NewLRUCache(1e6))
	m.DirMaxFiles = 10
	for i := 0; i < 1e3; i++ {
		m.Saver().AddAsync(Uint64Key(uint64(i)), []uint64{uint64(i)})
		time.Sleep(time.Millisecond * 10)
	}
	time.Sleep(time.Second)
	m.WalkDesc(clock.UnixNano()/1e6, func(m *Range) bool {
		if m != nil {
			fmt.Println(m.String())
		}
		return true
	})
}

func TestJump(t *testing.T) {

}
