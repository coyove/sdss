package bitmap

import (
	"bytes"
	"encoding/binary"

	"github.com/FastFilter/xorfilter"
)

func xfNew2(data []uint64) []byte {
	x, err := xorfilter.PopulateBinaryFuse8(data)
	if err != nil {
		panic(err)
	}
	p := &bytes.Buffer{}
	binary.Write(p, binary.BigEndian, x.Seed)               // 8b
	binary.Write(p, binary.BigEndian, x.SegmentLength)      // 4b
	binary.Write(p, binary.BigEndian, x.SegmentLengthMask)  // 4b
	binary.Write(p, binary.BigEndian, x.SegmentCount)       // 4b
	binary.Write(p, binary.BigEndian, x.SegmentCountLength) // 4b
	binary.Write(p, binary.BigEndian, x.Fingerprints)       // bytes
	return p.Bytes()
}

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

func xfBuild2(data []byte) xorfilter.BinaryFuse8 {
	x := xorfilter.BinaryFuse8{}
	x.Seed = binary.BigEndian.Uint64(data[:8])
	x.SegmentLength = binary.BigEndian.Uint32(data[8:12])
	x.SegmentLengthMask = binary.BigEndian.Uint32(data[12:16])
	x.SegmentCount = binary.BigEndian.Uint32(data[16:20])
	x.SegmentCountLength = binary.BigEndian.Uint32(data[20:24])
	x.Fingerprints = data[24:]
	return x
}

// Validness of 'data' is not checked.
func xfBuild(data []byte) xorfilter.Xor8 {
	x := xorfilter.Xor8{}
	x.Seed = binary.BigEndian.Uint64(data[:8])
	x.BlockLength = binary.BigEndian.Uint32(data[8:12])
	x.Fingerprints = data[12:]
	return x
}
