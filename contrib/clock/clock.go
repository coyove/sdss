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
	serverId   = uint32(Rand() * serverIdMask)
)

const (
	serverIdMask = 0x7fffffff
	tsOffset     = 1666666666
	encodeTable  = "-.0123456789_abcdefghijklmnopqrstuvwxyz~"
	// (2^64 - 40^12) / 2^64 = 0.1
	// Count time up to year 2220.
)

func init() {
	startupNano = runtimeNano()
	startupWallNano = time.Now().UnixNano()
}

func UnixNano() int64 {
	return runtimeNano() - startupNano + startupWallNano
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

	sec := Unix() - tsOffset
	if sec < idLastSec {
		panic("bad clock skew")
	}
	if sec != idLastSec {
		idCounter = 0
	}
	idLastSec = sec
	idCounter++
	if idCounter >= 0xffff {
		panic("too many calls in one second")
	}
	id = uint64(sec)<<31 | (uint64(serverId+idCounter) & uint64(serverIdMask))
	return
}

func IdStr() string {
	id := Id()
	buf := make([]byte, 12)
	for i := range buf {
		m := id % 40
		id = id / 40
		buf[len(buf)-i-1] = encodeTable[m]
	}
	return *(*string)(unsafe.Pointer(&buf))
}

func ParseTime(id uint64) int64 {
	return int64(id>>31) + tsOffset
}

func ParseTimeStr(idstr string) (int64, bool) {
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
	return ParseTime(id / 40), true
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
