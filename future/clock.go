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
	Block        int64 = 100e6                         // 10 blocks in one second
	channelWidth int64 = 8.32e6                        // 12 channels in one block
	Channels     int64 = Block / channelWidth          // total number of channels
	Margin       int64 = 8.20e6                        // NTP/PTP error must be less than half of +/-margin ms
	hi           int64 = (channelWidth - Margin) / 2   // 'hi' stores the timestamps of the current channel of curent block
	lo           int64 = (channelWidth - Margin) / 2   // 'lo' stores the overflowed timestamps of the same channel from previous block
	cookie       int64 = Block - Channels*channelWidth // 0.16ms
)

type channelState struct {
	last int64
	ctr  atomic.Int64
}

var (
	startup atomic.Pointer[record]
	atoms   [Channels]atomic.Pointer[channelState]
	Chrony  atomic.Pointer[chrony.ReplySourceStats]
	Base    atomic.Pointer[chrony.ReplySourceStats]
)

var test bool

func init() {
	reloadWallClock()
}

func reloadWallClock() {
	r := &record{
		Nano:     runtimeNano(),
		WallNano: time.Now().UnixNano(),
	}
	startup.Store(r)
}

func StartWatcher(onError func(error)) {
	if runtime.GOOS != "linux" {
		onError(fmt.Errorf("OS not supported, assume correct clock"))
		Base.CompareAndSwap(nil, &chrony.ReplySourceStats{})
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
		time.AfterFunc(time.Second*5, func() { StartWatcher(onError) })
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
			if int64(data.EstimatedOffsetErr*1e9) > Margin/2 {
				onError(fmt.Errorf("bad NTP clock %v, estimated error %v > %v",
					data.IPAddr,
					time.Duration(data.EstimatedOffsetErr*1e9), time.Duration(Margin/2)))
			} else {
				Base.CompareAndSwap(nil, data)

				diff := time.Now().UnixNano() - UnixNano()
				if !(-1e6 < diff && diff < 1e6) {
					onError(fmt.Errorf("mono clock differs from wall clock: %v", time.Duration(diff)))
					reloadWallClock()
				}
			}
			Chrony.Store(data)
			return
		}
	}

	Chrony.Store(nil)
	onError(fmt.Errorf("can't get source stats from chronyd"))
}

func UnixNano() int64 {
	r := startup.Load()
	return runtimeNano() - r.Nano + r.WallNano
}

func Now() time.Time {
	return time.Unix(0, UnixNano())
}

type Future int64

func Get(ch int64) Future {
	if ch < 0 || ch >= Channels {
		panic(fmt.Sprintf("invalid channel %d, out of range [0, %d)", ch, Channels))
	}
	if data := Base.Load(); data == nil {
		panic(fmt.Sprintf("bad NTP clock"))
	}

	ts := UnixNano()/Block*Block + ch*channelWidth

	if old := atoms[ch].Load(); old == nil || ts != old.last {
		// fmt.Println(ch, old, ts)
		if atoms[ch].CompareAndSwap(old, &channelState{
			last: ts,
			// ctr is 0,
		}) {
			if test {
				time.Sleep(time.Millisecond * 10)
			}
		}
	}

	upper := ts + channelWidth
	ts += atoms[ch].Load().ctr.Add(1)

	if ts >= upper-Margin-lo {
		panic(fmt.Sprintf("too many requests in %dms: %d >= %d - %d - %d",
			channelWidth/1e6, ts, upper, Margin, lo))
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
	return groupIdx / (channelWidth / 1e6)
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
