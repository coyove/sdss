package dal

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestBitmap(t *testing.T) {
	rand.Seed(time.Now().Unix())
	var x []uint64
	y := map[uint16]int{}
	for i := 0; i < 86400*100; i++ {
		v := rand.Uint64()
		x = append(x, v)
		y[uint16(v)]++
	}
	for k, v := range y {
		fmt.Println(k, v)
		break
	}
}
