package future

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
)

func TestNow(t *testing.T) {
	StartWatcher(func(e error) {
		fmt.Println(e)
	})

	for i := 0; i < 20; i++ {
		wg := sync.WaitGroup{}
		var tot int64
		const N = 5000
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func() {
				start := UnixNano()
				ts := Get(7) // int64(rand.Intn(Channels)))
				// fmt.Println(ts.Channel(), ts.IsFixed(), ts.Fixed(), Future(ts.Fixed()).IsFixed())
				v := uint16(rand.Int())
				v0, _ := ts.ToCookie(v).Cookie()
				if v0 != v {
					panic("invalid cookie")
				}
				ts.Wait()
				atomic.AddInt64(&tot, UnixNano()-start)
				wg.Done()
			}()
		}
		wg.Wait()
		fmt.Println(tot / N / 1e6)
	}
}
