package future

import (
	"fmt"
	"sync/atomic"
	"time"
	_ "unsafe"
)

//go:linkname runtimeNano runtime.nanotime
func runtimeNano() int64

var startup atomic.Pointer[record]

type record struct {
	Nano     int64
	WallNano int64
}

var (
	group  int64 = 40e6
	window int64 = 2.5e6
	margin int64 = 1e6 // safe margin is 1ms, which means NTP/PTP must offer 1ms accuracy
	tc     atomic.Int64
	last   atomic.Int64
)

func init() {
	reload()
}

func reload() {
	r := &record{
		Nano:     runtimeNano(),
		WallNano: time.Now().UnixNano(),
	}
	startup.Store(r)
	time.AfterFunc(time.Hour*24, reload)
}

func UnixNano() int64 {
	r := startup.Load()
	return runtimeNano() - r.Nano + r.WallNano
}

type Future int64

func Get(ch int64) Future {
	//                +------------+------------+
	//                |    ch 0    |    ch 1    |
	//  timeline ~ ~ -+------------+------------+- ~ ~
	// 		          |<- window ->|<- window ->|
	// 	              +------+-----+------+-----+
	// 	              | safe | 1ms | safe | 1ms |
	// 	              +------+--|--+------+-----+
	//                          +-- margin

	if ch < 0 || ch >= group/window {
		panic(fmt.Sprintf("invalid channel %d, out of range [0, %d)", ch, group/window))
	}

	ts := UnixNano()/group*group + ch*window

	if ts != last.Load() {
		if last.CompareAndSwap(last.Load(), ts) {
			tc.Store(0)
		}
	}

	upper := ts + window - margin
	ts += tc.Add(1)

	if UnixNano() < upper {
		return Future(ts)
	}

	for ts < UnixNano() {
		ts += group
	}
	return Future(ts)
}

func (f Future) Wait() {
	diff := time.Duration(int64(f) - UnixNano())
	time.Sleep(diff)
}
