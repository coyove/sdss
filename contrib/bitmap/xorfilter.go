package bitmap

import (
	"bytes"
	"encoding/binary"

	"github.com/FastFilter/xorfilter"
)

func xfNew(data []uint64) []byte {
	x, err := xorfilter.Populate(data)
	if err != nil {
		panic(err)
	}
	p := &bytes.Buffer{}
	binary.Write(p, binary.BigEndian, x.Seed)         // 8b
	binary.Write(p, binary.BigEndian, x.BlockLength)  // 4b
	binary.Write(p, binary.BigEndian, x.Fingerprints) // bytes
	return p.Bytes()
}

// Validness of 'data' is not checked.
func xfBuild(data []byte) xorfilter.Xor8 {
	x := xorfilter.Xor8{}
	x.Seed = binary.BigEndian.Uint64(data[:8])
	x.BlockLength = binary.BigEndian.Uint32(data[8:12])
	x.Fingerprints = data[12:]
	return x
}
