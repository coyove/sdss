package simple

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/coyove/sdss/contrib/clock"
)

func TestDedup(t *testing.T) {
	a := Uint64.Of([]int{1, 3, 2, 2})
	a = Uint64.Dedup(a)
	fmt.Println(a)

	rand.Seed(clock.UnixNano())
	for i := 0; i < 1e3; i++ {
		var a, b []uint64
		for i := 0; i < 1e3; i++ {
			a = append(a, rand.Uint64())
			b = append(b, rand.Uint64())
		}
		b = append(b, a[rand.Intn(len(a))])
		if !Uint64.ContainsAny(a, b) {
			t.Fatal()
		}
		if !Uint64.ContainsAny(b, a) {
			t.Fatal()
		}
	}
	for i := 0; i < 1e3; i++ {
		var a, b []uint64
		for i := 0; i < 1e3; i++ {
			a = append(a, rand.Uint64())
		}
		sort.Sort(&uint64Sort{a})
		b = append(b, a[0]-1, a[0], a[len(a)-1]+1)
		if !Uint64.ContainsAny(a, b) {
			t.Fatal()
		}
		if !Uint64.ContainsAny(b, a) {
			t.Fatal()
		}
	}
}
