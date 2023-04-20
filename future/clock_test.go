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
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				start := UnixNano()
				ts := TimestampID(7)
				SleepUntil(ts)
				atomic.AddInt64(&tot, UnixNano()-start)
				wg.Done()
			}()
		}
		wg.Wait()
		fmt.Println(tot / 100 / 1e6)
	}
}
