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

const (
	//  timeline -----+--------------+--------------+---->
	//                |     ch 0     |     ch 1     |
	//                +--------------+--------------+
	// 		          |<-  window  ->|<-  window  ->|
	// 	              +------+-------+------+-------+
	// 	              | safe | margin| safe | margin|
	// 	              +------+-------+------+-------+
	group    int64 = 100e6
	window   int64 = 10e6
	Margin   int64 = 9.5e6 // NTP/PTP must be +/-'margin'ms accurate
	Channels int64 = group / window
)

var (
	tc   atomic.Int64
	last atomic.Int64
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
	if ch < 0 || ch >= Channels {
		panic(fmt.Sprintf("invalid channel %d, out of range [0, %d)", ch, Channels))
	}

	ts := UnixNano()/group*group + ch*window

	if ts != last.Load() {
		if last.CompareAndSwap(last.Load(), ts) {
			tc.Store(0)
		}
	}

	upper := ts + window
	ts += tc.Add(1)

	if ts >= upper-Margin {
		panic(fmt.Sprintf("too many requests in %dms: %d >= %d - %d", window/1e6, ts, upper, Margin))
	}

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

func (f Future) Channel() int64 {
	ms := (int64(f) / 1e6) % 1e3
	groupIdx := ms % (group / 1e6)
	return groupIdx / (window / 1e6)
}
