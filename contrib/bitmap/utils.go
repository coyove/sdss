package bitmap

import (
	"math/bits"
)

func h16(v, offset uint32, deci int) (out [4]uint32) {
	out[0] = combinehash(v, uint32(deci))&0xfffff | offset
	out[1] = combinehash(v, out[0])&0xfffff | offset
	out[2] = combinehash(v, out[1])&0xfffff | offset
	out[3] = combinehash(v, out[2])&0xfffff | offset
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

// func (br *JoinedResult) Iterate(f func(ts int64, scores int) bool) {
// 	for i := 23; i >= 0; i-- {
// 		iter := br.hours[i].m.ReverseIterator()
// 		for iter.HasNext() {
// 			v := iter.Next()
// 			if !f(br.hours[i].baseTime+int64(v), int(br.hours[i].scores[v])) {
// 				break
// 			}
// 		}
// 	}
// }

// func (r *JoinedResult) Subtract(r2 *JoinedResult) {
// 	for i := 23; i >= 0; i-- {
// 		x := &r.hours[i]
// 		if x.baseTime != r2.hours[i].baseTime {
// 			panic("JoinedResult.Subtract: unmatched base time")
// 		}
// 		x.m.AndNot(r2.hours[i].m)
// 		for k := range x.scores {
// 			if !x.m.Contains(k) {
// 				delete(x.scores, k)
// 			}
// 		}
// 	}
// }
