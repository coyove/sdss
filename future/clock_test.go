package future

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestNow(t *testing.T) {
	for i := 0; i < 20; i++ {
		wg := sync.WaitGroup{}
		var tot int64
		const N = 5000
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func() {
				start := UnixNano()
				ts := Get(7)
				// fmt.Println(ts.Channel())
				ts.Wait()
				atomic.AddInt64(&tot, UnixNano()-start)
				wg.Done()
			}()
		}
		wg.Wait()
		fmt.Println(tot / N / 1e6)
	}
}
