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
	//  timeline -----+--------------+--------------+~~~+--------------+--------+---->
	//                |                           Block                         |
	//                +--------------+--------------+~~~+--------------+--------+---->
	//                |     ch 0     |     ch 1     |   |     ch 11    |        |
	//                +--------------+--------------+~~~+--------------+        |
	// 		          |    Window    |    Window    |   |    Window    | cookie |
	// 	              +-----+--------+-----+--------+~~~+-----+--------+        |
	// 	              |lo|hi| Margin |lo|hi| Margin |   |lo|hi| Margin |        |
	// 	              +-----+--------+-----+--------+~~~+-----+--------+--------+
	Block    int64 = 125e6
	Window   int64 = 10.4e6                  // all timestamps fall into the current window will be stored in 'hi'
	Margin   int64 = 10.1e6                  // NTP/PTP must be +/-'margin'ms accurate
	hi       int64 = (Window - Margin) / 2   // 'hi' stores the current timestamps of the current window
	lo       int64 = (Window - Margin) / 2   // 'lo' stores the overflowed timestamps of the same window from previous block
	Channels int64 = Block / Window          // total number of channels
	cookie   int64 = Block - Channels*Window // 0.2ms
)

var (
	startup atomic.Pointer[record]
	tc      [Channels]atomic.Int64
	last    [Channels]atomic.Int64
	bad     atomic.Pointer[chrony.ReplySourceStats]
	Chrony  atomic.Pointer[chrony.ReplySourceStats]
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
			Chrony.Store(data)
			return
		}
	}

	bad.Store(nil)
	Chrony.Store(nil)
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

	ts := UnixNano()/Block*Block + ch*Window

	if old := last[ch].Load(); ts != old {
		// fmt.Println(ch, old, ts)
		if last[ch].CompareAndSwap(old, ts) {
			tc[ch].Store(0)
		}
	}

	upper := ts + Window
	ts += tc[ch].Add(1)

	if ts >= upper-Margin-lo {
		panic(fmt.Sprintf("too many requests in %dms: %d >= %d - %d - %d",
			Window/1e6, ts, upper, Margin, lo))
	}

	if UnixNano() < upper {
		return Future(ts + lo)
	}

	for ts < UnixNano() {
		ts += Block
	}
	return Future(ts)
}

func (f Future) Wait() {
	diff := time.Duration(int64(f) - UnixNano())
	time.Sleep(diff)
}

func (f Future) Channel() int64 {
	ms := (int64(f) / 1e6) % 1e3
	groupIdx := ms % (Block / 1e6)
	return groupIdx / (Window / 1e6)
}

func (f Future) Cookie() (uint16, bool) {
	next := int64(f)/Block*Block + Block
	if int64(f) >= next-cookie && int64(f) < next {
		return uint16(next - int64(f) - 1), true
	}
	return 0, false
}

func (f Future) ToCookie(c uint16) Future {
	grouped := int64(f) / Block * Block
	return Future(grouped + Block - 1 - int64(c))
}
