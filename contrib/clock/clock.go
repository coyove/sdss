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

//go:linkname now time.now
func now() (sec int64, nsec int32, mono int64)

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
	serverIdMask = 0xfffffff
	maxCounter   = 0x3fff
	tsBits       = 28
	tsOffset     = 16666666666
	encodeTable  = "-.0123456789_abcdefghijklmnopqrstuvwxyz~"
	// (2^64 - 40^12) / 2^64 = 0.1
	// Count time up to year 2220.
)

func init() {
	startupNano = runtimeNano()
	startupWallNano = time.Now().UnixNano()
}

func ServerId() int {
	return int(serverId >> 15)
}

func UnixNano() int64 {
	return runtimeNano() - startupNano + startupWallNano
}

func UnixDeci() int64 {
	return UnixNano() / 1e8
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

	sec := UnixDeci() - tsOffset
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
	id = uint64(sec)<<tsBits | serverId | uint64(idCounter)
	return
}

func IdStr() string {
	return Base40Encode(Id())
}

func Base40Encode(id uint64) string {
	buf := make([]byte, 12)
	for i := range buf {
		m := id % 40
		id = id / 40
		buf[len(buf)-i-1] = encodeTable[m]
	}
	return *(*string)(unsafe.Pointer(&buf))
}

func UnixDeciToIdStr(m int64) string {
	return Base40Encode(uint64(m-tsOffset) << tsBits)
}

func ParseIdUnixDeci(id uint64) int64 {
	return int64(id>>tsBits) + tsOffset
}

func Base40Decode(idstr string) (uint64, bool) {
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
	return id / 40, true
}

func ParseIdStrUnix(idstr string) (int64, bool) {
	id, ok := Base40Decode(idstr)
	return ParseIdUnixDeci(id) / 10, ok
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
