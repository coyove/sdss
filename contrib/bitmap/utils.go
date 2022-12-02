package bitmap

import (
	"bytes"
	"fmt"
	"io"
	"math/bits"
	"os"
)

const (
	JoinAll = iota
	JoinOne
	JoinMajor
)

type meterWriter struct {
	io.Writer
	size int
}

func (m *meterWriter) Write(p []byte) (int, error) {
	n, err := m.Writer.Write(p)
	m.size += n
	return n, err
}

func h16(v uint32, ts int64) (out [4]uint32) {
	const mask = 0xffffffff
	out[0] = combinehash(v, uint32(ts)) & mask
	out[1] = combinehash(v, out[0]) & mask
	out[2] = combinehash(v, out[1]) & mask
	out[3] = combinehash(v, out[2]) & mask
	return
}

func combinehash(k1, seed uint32) uint32 {
	h1 := seed

	k1 *= 0xcc9e2d51
	k1 = bits.RotateLeft32(k1, 15)
	k1 *= 0x1b873593

	h1 ^= k1
	h1 = bits.RotateLeft32(h1, 13)
	h1 = h1*4 + h1 + 0xe6546b64

	h1 ^= uint32(4)

	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}

type KeyTimeScore struct {
	Key      uint64
	UnixDeci int64
	Score    int
}

func (kts KeyTimeScore) Unix() int64 {
	return kts.UnixDeci / 10
}

func (b *Day) Save(path string) (int, error) {
	b.mfmu.Lock()
	defer b.mfmu.Unlock()

	bakpath := fmt.Sprintf("%s.%d.mtfbak", path, b.BaseTime())

	f, err := os.Create(bakpath)
	if err != nil {
		return 0, err
	}
	sz, err := b.Marshal(f)
	f.Close()
	if err != nil {
		return 0, err
	}

	if err := os.Remove(path); err != nil {
		if !os.IsNotExist(err) {
			return 0, err
		}
	}
	return sz, os.Rename(bakpath, path)
}

func Load(path string) (*Day, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	return Unmarshal(f)
}

type bitmap1440 [24]uint64

func (b *bitmap1440) add(min uint16) { // [0, 1440)
	hr := min / 60
	min = min % 60
	(*b)[hr] |= 1 << min
}

func (b *bitmap1440) contains(hr int, secDeci uint32) bool {
	min := secDeci / 10 / 60
	return (*b)[hr]&(1<<(min%60)) > 0
}

func (b bitmap1440) String() string {
	buf := &bytes.Buffer{}
	for i := 0; i < 24; i++ {
		count := 0
		for m := 0; m < 60; m++ {
			if b[i]&(1<<m) > 0 {
				fmt.Fprintf(buf, "%02d:%02d ", i, m)
				count++
			}
		}
		if count > 0 {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}

func majorScore(s int) int {
	if s <= 2 {
		return s
	}
	if s <= 4 {
		return s - 1
	}
	return s * 4 / 5
}

func dedupUint32(qs, musts []uint32) ([]uint32, []uint32) {
	f1 := func(a []uint32) []uint32 {
		if len(a) <= 1 {
			return a
		}
		if len(a) == 2 {
			if a[0] != a[1] {
				return a
			}
			return a[:1]
		}
		m := make(map[uint32]struct{}, len(a))
		for i := len(a) - 1; i >= 0; i-- {
			if _, ok := m[a[i]]; ok {
				a = append(a[:i], a[i+1:]...)
			}
			m[a[i]] = struct{}{}
		}
		return a
	}

	qs, musts = f1(qs), f1(musts)
	if len(qs) == 0 || len(musts) == 0 {
		return qs, musts
	}

	for i := len(qs) - 1; i >= 0; i-- {
		for _, v := range musts {
			if v == qs[i] {
				qs = append(qs[:i], qs[i+1:]...)
				break
			}
		}
	}
	return qs, musts
}
