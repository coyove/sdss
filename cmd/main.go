package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/coyove/sdss"
)

func main() {
	const N = 1e5
	if true {
		m := sdss.NewMap(0)
		for i := 0; i < N; i++ {
			k, v := rand.Uint64(), rand.Float64()
			m.Put(k, v)
		}
		fmt.Println(m.Cap())
	} else {
		m2 := map[uint64]float64{}
		for i := 0; i < N; i++ {
			k, v := rand.Uint64(), rand.Float64()
			m2[k] = v
		}
	}

	time.Sleep(time.Hour)
}
