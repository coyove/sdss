package bitmap

import (
	"bytes"
	"encoding/binary"
	"unsafe"

	"github.com/FastFilter/xorfilter"
)

func xfNew(data []uint64) []byte {
	if len(data) == 0 {
		panic("empty data")
	}
	p := &bytes.Buffer{}
	if len(data) <= 6 {
		binary.Write(p, binary.BigEndian, uint32(0))
		var buf []byte
		*(*[3]int)(unsafe.Pointer(&buf)) = [3]int{
			*(*int)(unsafe.Pointer(&data)),
			len(data) * 8,
			len(data) * 8,
		}
		p.Write(buf)
		return p.Bytes()
	}

	data = append(data, data...)
	for i, half := 0, len(data)/2; i < half; i++ {
		data[half+i] = ^data[i]
	}
	x, err := xorfilter.Populate(data)
	if err != nil {
		panic(err)
	}
	binary.Write(p, binary.BigEndian, x.BlockLength) // 4b
	binary.Write(p, binary.BigEndian, x.Seed)        // 8b
	p.Write(x.Fingerprints)                          // bytes
	return p.Bytes()
}

// Validness of 'data' is not checked.
func xfBuild(data []byte) (xorfilter.Xor8, []uint64) {
	x := xorfilter.Xor8{}
	x.BlockLength = binary.BigEndian.Uint32(data[:4])
	if x.BlockLength == 0 {
		var values []uint64
		l := len(data) - 4
		*(*[3]int)(unsafe.Pointer(&values)) = [3]int{
			int(uintptr(unsafe.Pointer(&data[4]))),
			l / 8,
			l / 8,
		}
		return x, values
	}
	x.Seed = binary.BigEndian.Uint64(data[4:12])
	x.Fingerprints = data[12:]
	return x, nil
}

func xfContains(x xorfilter.Xor8, vs []uint64, v uint64) bool {
	if len(vs) == 0 {
		return x.Contains(v) && x.Contains(^v)
	}
	for _, v0 := range vs {
		if v0 == v {
			return true
		}
	}
	return false
}
