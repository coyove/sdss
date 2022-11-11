package clock

import (
	"math/rand"
	_ "runtime"
	"strings"
	"sync"
	"time"
	"unsafe"
	_ "unsafe"
)

//go:linkname runtimeNano runtime.nanotime
func runtimeNano() int64

var (
	startupNano     int64
	startupWallNano int64

	idCounter uint32
	idLastSec int64
	idMutex   sync.Mutex

	randMu     sync.Mutex
	randSeeded bool
	serverId   = uint64(Rand()*(serverIdMask-maxCounter)) & (serverIdMask - maxCounter)
)

const (
	serverIdMask = 0x1fffff
	maxCounter   = 0x1ff
	tsOffset     = 1666666666666
	encodeTable  = "-.0123456789_abcdefghijklmnopqrstuvwxyz~"
	// (2^64 - 40^12) / 2^64 = 0.1
	// Count time up to year 2220.
)

func init() {
	startupNano = runtimeNano()
	startupWallNano = time.Now().UnixNano()
}

func ServerId() int {
	return int(serverId >> 9)
}

func UnixNano() int64 {
	return runtimeNano() - startupNano + startupWallNano
}

func UnixMilli() int64 {
	return UnixNano() / 1e6
}

func Unix() int64 {
	return UnixNano() / 1e9
}

func Now() time.Time {
	return time.Unix(0, UnixNano())
}

func Id() (id uint64) {
	idMutex.Lock()
	defer idMutex.Unlock()

	sec := UnixMilli() - tsOffset
	if sec < idLastSec {
		panic("bad clock skew")
	}
	if sec != idLastSec {
		idCounter = 0
	}
	idLastSec = sec
	idCounter++
	if idCounter >= maxCounter {
		panic("too many IDs generated in 1ms")
	}
	id = uint64(sec)<<21 | serverId | uint64(idCounter)
	return
}

func IdStr() string {
	return base40Encode(Id())
}

func base40Encode(id uint64) string {
	buf := make([]byte, 12)
	for i := range buf {
		m := id % 40
		id = id / 40
		buf[len(buf)-i-1] = encodeTable[m]
	}
	return *(*string)(unsafe.Pointer(&buf))
}

func UnixMilliToIdStr(m int64) string {
	return base40Encode(uint64(m-tsOffset) << 21)
}

func ParseUnixMilli(id uint64) int64 {
	return int64(id>>21) + tsOffset
}

func ParseStrUnixMilli(idstr string) (int64, bool) {
	if len(idstr) != 12 {
		return 0, false
	}

	var id uint64
	for i := range idstr {
		idx := strings.IndexByte(encodeTable, idstr[i])
		if idx < 0 {
			return 0, false
		}
		id = (id + uint64(idx)) * 40
	}
	return ParseUnixMilli(id / 40), true
}

func Rand() float64 {
	randMu.Lock()
	if !randSeeded {
		rand.Seed(UnixNano())
		randSeeded = true
	}
	v := rand.Float64()
	randMu.Unlock()
	return v
}
