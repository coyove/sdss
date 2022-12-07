package cursor

import (
	"bytes"
	"fmt"

	"github.com/coyove/sdss/contrib/clock"
)

type CursorManager struct {
	Get func(string) ([]byte, bool)
	Set func(string, []byte)
}

func (cm *CursorManager) Save(c *Cursor) string {
	key := fmt.Sprintf("%x!%x!%x", c.NextMap, c.NextId, clock.Id())
	cm.Set(key, c.MarshalBinary())
	return key
}

func (cm *CursorManager) Load(k string) (c *Cursor, err error) {
	data, ok := cm.Get(k)
	if !ok {
		var nm, ni, tmp int64
		n, _ := fmt.Sscanf(k, "%x!%x!%x", &nm, &ni, &tmp)
		if n != 3 {
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
