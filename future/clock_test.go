package future

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestNow(t *testing.T) {
	for i := 0; i < 50; i++ {
		wg := sync.WaitGroup{}
		var tot int64
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				start := UnixNano()
				ts := Get(7)
				ts.Wait()
				atomic.AddInt64(&tot, UnixNano()-start)
				wg.Done()
			}()
		}
		wg.Wait()
		fmt.Println(tot / 100 / 1e6)
	}
}
