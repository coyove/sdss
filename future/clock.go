package future

import (
	"fmt"
	"net"
	"runtime"
	"sync/atomic"
	"time"
	_ "unsafe"

	"github.com/coyove/sdss/future/chrony"
)

//go:linkname runtimeNano runtime.nanotime
func runtimeNano() int64

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
	window   int64 = 12.5e6
	Margin   int64 = 10.5e6 // NTP/PTP must be +/-'margin'ms accurate
	Channels int64 = group / window
)

var (
	startup atomic.Pointer[record]
	tc      atomic.Int64
	last    atomic.Int64
	bad     atomic.Pointer[chrony.ReplySourceStats]
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

func StartWatcher(onError func(error)) {
	if runtime.GOOS != "linux" {
		onError(fmt.Errorf("OS not supported, assume correct clock"))
		return
	}

	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				onError(err)
			} else {
				onError(fmt.Errorf("watcher panic: %v", r))
			}
		}
		time.AfterFunc(time.Second*10, func() { StartWatcher(onError) })
	}()

	conn, err := net.Dial("udp", ":323")
	if err != nil {
		onError(err)
		return
	}
	defer conn.Close()

	client := &chrony.Client{Connection: conn, Sequence: 1}
	resp, err := client.Communicate(chrony.NewTrackingPacket())
	if err != nil {
		onError(err)
		return
	}
	refId := resp.(*chrony.ReplyTracking).RefID

	for i := 0; ; i++ {
		resp, err := client.Communicate(chrony.NewSourceStatsPacket(int32(i)))
		if err != nil {
			break
		}

		data := resp.(*chrony.ReplySourceStats)
		if data.RefID == refId {
			if int64(data.EstimatedOffsetErr*1e9) > Margin {
				bad.Store(data)
				onError(fmt.Errorf("bad NTP clock %v, estimated error %v > %v",
					data.IPAddr,
					time.Duration(data.EstimatedOffsetErr*1e9), time.Duration(Margin)))
			} else {
				bad.Store(nil)
			}
			return
		}
	}

	bad.Store(nil)
	onError(fmt.Errorf("can't get source stats from chronyd"))
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
	if data := bad.Load(); data != nil {
		panic(fmt.Errorf("bad NTP clock %v, estimated error %v > %v",
			data.IPAddr,
			time.Duration(data.EstimatedOffsetErr*1e9), time.Duration(Margin)))
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
