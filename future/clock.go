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
	group int64 = 1e9
	tc    atomic.Int64
	last  atomic.Int64
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

func TimestampID(ch int64) int64 {
	if ch < 0 || ch >= group/4e6 {
		panic(fmt.Sprintf("invalid channel %d, out of range [0, %d)", ch, group/4e6))
	}

	ts := UnixNano()/group*group + (ch*4+1)*1e6

	if ts != last.Load() {
		if last.CompareAndSwap(last.Load(), ts) {
			tc.Store(0)
		}
	}

	ts += tc.Add(1)
	for ts < UnixNano() {
		ts += group
	}

	return ts
}

func SleepUntil(ts int64) {
	diff := time.Duration(ts - UnixNano())
	time.Sleep(diff)
}
