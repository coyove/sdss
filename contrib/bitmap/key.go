package bitmap

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"unsafe"
)

const KeySize = int(unsafe.Sizeof(Key{}))

type Key [16]byte

func BytesKey(v []byte) (k Key) {
	copy(k[:], v)
	return
}

func Uint64Key(v uint64) (k Key) {
	binary.BigEndian.PutUint64(k[8:], v)
	return
}

func Uint64HighLowKey(hi, lo uint64) (k Key) {
	binary.BigEndian.PutUint64(k[:8], hi)
	binary.BigEndian.PutUint64(k[8:], lo)
	return
}

func ObjectIdHexKey(v string) (k Key) {
	if len(v) == 24 {
		hex.Decode(k[:4], []byte(v))
	}
	return
}

func (k Key) HighUint64() uint64 {
	return binary.BigEndian.Uint64(k[:8])
}

func (k Key) LowUint64() uint64 {
	return binary.BigEndian.Uint64(k[8:])
}

func (k Key) Less(k2 Key) bool {
	return bytes.Compare(k[:], k2[:]) < 0
}

func (k Key) String() string {
	return hex.EncodeToString(k[:])
}

func (k Key) Incr() Key {
	for i := len(k) - 1; i >= 0; i-- {
		k[i]++
		if k[i] > 0 {
			break
		}
	}
	return k
}

func keysBytes(keys []Key) (x []byte) {
	*(*[3]int)(unsafe.Pointer(&x)) = [3]int{
		*(*int)(unsafe.Pointer(&keys)),
		len(keys) * KeySize,
		len(keys) * KeySize,
	}
	return
}

func bytesKeys(buf []byte) (x []Key) {
	*(*[3]int)(unsafe.Pointer(&x)) = [3]int{
		*(*int)(unsafe.Pointer(&buf)),
		len(buf) / KeySize,
		len(buf) / KeySize,
	}
	return
}
