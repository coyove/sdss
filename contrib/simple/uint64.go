package simple

import "sort"

var Uint64 struct {
	Dedup    func([]uint64) []uint64
	Contains func([]uint64, uint64) bool
	Equal    func([]uint64, []uint64) bool
}

func init() {
	Uint64.Dedup = func(v []uint64) []uint64 {
		if len(v) <= 1 {
			return v
		}
		if len(v) == 2 {
			if v[0] == v[1] {
				return v[:1]
			}
			return v
		}
		s := uint64Sort{v}
		sort.Sort(s)
		for i := len(v) - 1; i > 0; i-- {
			if v[i] == v[i-1] {
				v = append(v[:i], v[i+1:]...)
			}
		}
		return v
	}

	Uint64.Contains = func(a []uint64, b uint64) bool {
		if len(a) < 10 {
			for _, v := range a {
				if v == b {
					return true
				}
			}
			return false
		}
		s := uint64Sort{a}
		if !sort.IsSorted(s) {
			sort.Sort(s)
		}
		idx := sort.Search(len(a), func(i int) bool { return a[i] >= b })
		return idx < len(a) && a[idx] == b
	}

	Uint64.Equal = func(a, b []uint64) bool {
		if len(a) != len(b) {
			return false
		}
		sort.Sort(uint64Sort{a})
		sort.Sort(uint64Sort{b})
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
}

type uint64Sort struct{ data []uint64 }

func (h uint64Sort) Len() int { return len(h.data) }

func (h uint64Sort) Less(i, j int) bool { return h.data[i] < h.data[j] }

func (h uint64Sort) Swap(i, j int) { h.data[i], h.data[j] = h.data[j], h.data[i] }
