package dal

import (
	"sort"
	"sync"

	"github.com/coyove/sdss/contrib/clock"
)

var cm struct {
	mu sync.Mutex
	m  map[string]string
}

func addDoc(id string, content string) {
	cm.mu.Lock()
	if cm.m == nil {
		cm.m = map[string]string{}
	}
	cm.m[id] = content
	cm.mu.Unlock()
}

func getDoc(id string) (content string) {
	cm.mu.Lock()
	content = cm.m[id]
	cm.mu.Unlock()
	return
}

func scanDoc(milli int64) (ids []string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	lower := clock.UnixToIdStr(milli)
	upper := clock.UnixToIdStr(milli + 1)
	for k := range cm.m {
		if k >= lower && k < upper {
			ids = append(ids, k)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	return
}

type int64Heap struct {
	data []int64
}

func (h *int64Heap) Len() int {
	return len(h.data)
}

func (h *int64Heap) Less(i, j int) bool {
	return h.data[i] < h.data[j]
}

func (h *int64Heap) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *int64Heap) Push(x interface{}) {
	h.data = append(h.data, x.(int64))
}

func (h *int64Heap) Pop() interface{} {
	old := h.data
	n := len(old)
	x := old[n-1]
	h.data = old[:n-1]
	return x
}
