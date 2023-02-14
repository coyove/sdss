package bitmap

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coyove/sdss/contrib/clock"
	"golang.org/x/sync/singleflight"
)

type Manager struct {
	mu, reloadmu sync.Mutex
	dirname      string
	switchLimit  int64
	dirFiles     []string
	current      *SaveAggregator
	currentRO    *Range
	loader       singleflight.Group
	cache        *Cache

	DirMaxFiles int

	Event struct {
		OnLoaded  func(string, time.Duration)
		OnSaved   func(string, int, error, time.Duration)
		OnMissing func(int64) (*Range, error)
	}
}

func (m *Manager) getPath(base int64) string {
	return filepath.Join(m.dirname, fmt.Sprintf("%016x", base))
}

func (m *Manager) saveAggImpl(b *Range) error {
	start := time.Now()
	m.currentRO = b.Clone()
	fn := m.getPath(b.Start())
	x, err := b.Save(fn, b.Len() >= m.switchLimit)
	if err == nil {
		if bs, ok := m.Last(); !ok || bs != b.Start() {
			err = m.ReloadFiles()
		}
	}
	if m.Event.OnSaved != nil {
		m.Event.OnSaved(fn, x, err, time.Since(start))
	}
	return err
}

func (m *Manager) load(offset int64) (*Range, error) {
	if offset == m.currentRO.Start() {
		return m.currentRO, nil
	}
	fn := m.getPath(offset)
	cached := m.cache.Get(fn)
	if cached != nil {
		return cached, nil
	}
	out, err, _ := m.loader.Do(fn, func() (interface{}, error) {
		start := time.Now()
		v, err := Load(fn)
		if v == nil && err == nil {
			return nil, nil
		}
		if m.Event.OnLoaded != nil {
			m.Event.OnLoaded(fn, time.Since(start))
		}
		return v, err
	})
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	m.cache.Add(fn, out.(*Range))
	return out.(*Range), nil
}

func (m *Manager) findNext(mark int64) (int64, bool) {
	marks := fmt.Sprintf("%016x", mark)
	idx := sort.SearchStrings(m.dirFiles, marks)
	if idx >= len(m.dirFiles) {
		return 0, true
	}
	if m.dirFiles[idx] == marks {
		idx++
	}
	prev, _ := strconv.ParseInt(m.dirFiles[idx], 16, 64)
	return prev, false
}

func (m *Manager) findPrev(mark int64) (int64, bool) {
	marks := fmt.Sprintf("%016x", mark)
	idx := sort.SearchStrings(m.dirFiles, marks)
	if idx >= len(m.dirFiles) {
		idx = len(m.dirFiles)
	}
	if idx == 0 {
		return 0, true
	}
	prev, _ := strconv.ParseInt(m.dirFiles[idx-1], 16, 64)
	return prev, false
}

func (m *Manager) Last() (int64, bool) {
	m.reloadmu.Lock()
	defer m.reloadmu.Unlock()
	v, empty := m.findPrev(clock.UnixMilli() + 1)
	return v, !empty
}

func (m *Manager) ReloadFiles() error {
	m.reloadmu.Lock()
	defer m.reloadmu.Unlock()
	df, err := os.Open(m.dirname)
	if err != nil {
		return err
	}
	defer df.Close()
	names, err := df.Readdirnames(-1)
	if err != nil {
		return err
	}

	for i := len(names) - 1; i >= 0; i-- {
		if strings.HasSuffix(names[i], ".mtfbak") {
			names = append(names[:i], names[i+1:]...)
		}
	}

	sort.Strings(names)
	if m.DirMaxFiles > 0 {
		for len(names) > m.DirMaxFiles {
			os.Remove(filepath.Join(m.dirname, names[0]))
			names = names[1:]
		}
	}

	for _, n := range names {
		if _, err := strconv.ParseInt(n, 16, 64); err != nil {
			return fmt.Errorf("invalid filename %s/%s: %v", m.dirname, n, err)
		}
	}
	m.dirFiles = names
	return nil
}

func NewManager(dir string, switchLimit int64, cache *Cache) (*Manager, error) {
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	if cache == nil {
		cache = NewLRUCache(0)
	}
	m := &Manager{
		dirname:     dir,
		cache:       cache,
		switchLimit: switchLimit,
	}
	if err := m.ReloadFiles(); err != nil {
		return nil, err
	}

	normBase := clock.UnixMilli()
	prevBase, isEmpty := m.findPrev(normBase + 1)
	if isEmpty {
		m.current = New(normBase).AggregateSaves(m.saveAggImpl)
	} else {
		b, err := Load(m.getPath(prevBase))
		if err != nil {
			return nil, err
		}
		m.current = b.AggregateSaves(m.saveAggImpl)
	}
	m.currentRO = m.current.Range().Clone()
	return m, nil
}

func (m *Manager) Saver() *SaveAggregator {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current.Range().Len() >= m.switchLimit {
		m.current.Close()
		m.current = New(clock.UnixMilli()).AggregateSaves(m.saveAggImpl)
		m.currentRO = m.current.Range().Clone()
	}
	return m.current
}

func (m *Manager) WalkAsc(start int64, f func(*Range) bool) (err error) {
	for {
		if start == 0 {
			// Since we use unix milli as the filename, 0 can't be a legal one.
			start = 1
		}
		next, isLast := m.findNext(start - 1)
		if isLast {
			return io.EOF
		}
		b, err := m.load(next)
		if err != nil {
			return err
		}
		if b != nil && !f(b) {
			return nil
		}
		start = next + 1
	}
}

func (m *Manager) WalkDesc(start int64, f func(*Range) bool) (err error) {
	for {
		var b *Range

		prev, isFirst := m.findPrev(start + 1)
		if isFirst {
			if m.Event.OnMissing != nil {
				b, err = m.Event.OnMissing(start + 1)
				goto LOADED
			}
			return io.EOF
		}
		b, err = m.load(prev)

	LOADED:
		if err != nil {
			return err
		}
		if b != nil && !f(b) {
			return nil
		}
		start = prev - 1
	}
}

func (m *Manager) String() string {
	return fmt.Sprintf("files: %d, saver: %.1f, cache: %d(%db)",
		len(m.dirFiles), m.current.Metrics(), m.cache.Len(), m.cache.curWeight)
}

func (m *Manager) CollectSimple(dedup interface{ Add(Key) bool }, vs Values, n int) (res []KeyIdScore, jms []JoinMetrics) {
	m.WalkDesc(clock.UnixMilli(), func(b *Range) bool {
		jm := b.Join(vs, -1, true, func(kis KeyIdScore) bool {
			if dedup.Add(kis.Key) {
				res = append(res, kis)
			}
			return len(res) < n
		})
		jms = append(jms, jm)
		return len(res) < n
	})
	return
}
