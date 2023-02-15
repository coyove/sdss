package clock

import (
	"math"
	"math/rand"
	_ "runtime"
	"strings"
	"sync"
	"time"
	"unsafe"
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

	randMu   sync.Mutex
	serverId = uint64(Rand()*math.MaxUint32) & (serverIdMask - maxCounter)
)

const (
	serverIdMask = 0x3ffffff
	maxCounter   = 0x1fff
	tsBits       = 26
	tsOffset     = 1666666666
	encodeTable  = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	// log2(62^10) = 59 -> 33 + 26
)

func init() {
	rand.Seed(UnixNano())
	startupNano = runtimeNano()
	startupWallNano = time.Now().UnixNano()
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

	sec := Unix() - tsOffset
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
	return Base62Encode(Id())
}

func Base62Encode(id uint64) string {
	buf := make([]byte, 10)
	for i := range buf {
		m := id % 62
		id = id / 62
		buf[len(buf)-i-1] = encodeTable[m]
	}
	return *(*string)(unsafe.Pointer(&buf))
}

func UnixToIdStr(m int64) string {
	return Base62Encode(uint64(m-tsOffset) << tsBits)
}

func ParseIdUnix(id uint64) int64 {
	return int64(id>>tsBits) + tsOffset
}

func Base62Decode(idstr string) (uint64, bool) {
	if len(idstr) != 10 {
		return 0, false
	}

	var id uint64
	for i := range idstr {
		idx := strings.IndexByte(encodeTable, idstr[i])
		if idx < 0 {
			return 0, false
		}
		id = (id + uint64(idx)) * 62
	}
	return id / 62, true
}

func ParseIdStrUnix(idstr string) (int64, bool) {
	id, ok := Base62Decode(idstr)
	return ParseIdUnix(id), ok
}

func Rand() float64 {
	randMu.Lock()
	v := rand.Float64()
	randMu.Unlock()
	return v
}
