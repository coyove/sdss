package cursor

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/coyove/sdss/contrib/clock"
)

type CursorManager struct {
	idx, step int64
	init      sync.Once
	Get       func(string) ([]byte, bool)
	Set       func(string, []byte)
}

func (cm *CursorManager) Save(c *Cursor) string {
	cm.init.Do(func() { cm.step = clock.Unix() % 16 })
	tot := 0
	for _, c := range c.compacts {
		tot += len(c.Fingerprints)
	}
	key := fmt.Sprintf("%x!%x!%x!%d.%d.%d", c.NextMap, c.NextId, atomic.AddInt64(&cm.idx, cm.step),
		len(c.pendings), len(c.compacts), tot)
	cm.Set(key, c.MarshalBinary())
	return key
}

func (cm *CursorManager) Load(k string) (c *Cursor, err error) {
	data, ok := cm.Get(k)
	if !ok {
		var nm, ni, tmp int64
		n, _ := fmt.Sscanf(k, "%x!%x!%x!%d.%d", &nm, &ni, &tmp, &tmp, &tmp)
		if n != 5 {
			return nil, fmt.Errorf("cursor manager: invalid key")
		}
		c = New()
		c.NextMap = nm
		c.NextId = ni
		return c, nil
	}
	c, ok = Read(bytes.NewReader(data))
	if !ok {
		return nil, fmt.Errorf("cursor manager: invalid key data")
	}
	return c, nil
}
