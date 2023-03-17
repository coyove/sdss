package simple

import (
	"reflect"
	"sort"
)

var Uint64 struct {
	Dedup       func([]uint64) []uint64
	Contains    func([]uint64, uint64) bool
	ContainsAny func([]uint64, []uint64) bool
	Equal       func([]uint64, []uint64) bool
	Of          func(interface{}) []uint64
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

	Uint64.ContainsAny = func(a []uint64, b []uint64) bool {
		if len(a)+len(b) < 10 {
			for _, v := range a {
				for _, v2 := range b {
					if v == v2 {
						return true
					}
				}
			}
			return false
		}
		sa := uint64Sort{a}
		sb := uint64Sort{b}
		if !sort.IsSorted(sa) {
			sort.Sort(sa)
		}
		if !sort.IsSorted(sb) {
			sort.Sort(sb)
		}

		for _, b := range b {
			idx := sort.Search(len(a), func(i int) bool { return a[i] >= b })
			if idx < len(a) {
				if a[idx] == b {
					return true
				}
				if idx > 0 {
					a = a[idx-1:]
				}
			} else {
				break
			}
		}
		return false
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

	Uint64.Of = func(in interface{}) (res []uint64) {
		rv := reflect.ValueOf(in)
		res = make([]uint64, rv.Len())
		for i := range res {
			switch el := rv.Index(i); el.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				res[i] = uint64(el.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
				res[i] = el.Uint()
			case reflect.Float32, reflect.Float64:
				res[i] = uint64(el.Float())
			}
		}
		return res
	}
}

type uint64Sort struct{ data []uint64 }

func (h uint64Sort) Len() int { return len(h.data) }

func (h uint64Sort) Less(i, j int) bool { return h.data[i] < h.data[j] }

func (h uint64Sort) Swap(i, j int) { h.data[i], h.data[j] = h.data[j], h.data[i] }
