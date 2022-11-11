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
	lower := clock.UnixMilliToIdStr(milli)
	upper := clock.UnixMilliToIdStr(milli + 1)
	for k := range cm.m {
		if k >= lower && k < upper {
			ids = append(ids, k)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	return
}
