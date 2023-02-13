package bitmap

import (
	"fmt"
	"strconv"
	"testing"
)

func TestCache_Add(t *testing.T) {
	base := New(0).RoughSizeBytes()

	c := NewLRUCache(10 * base)

	for i := 0; i < 10; i++ {
		c.Add("key"+strconv.Itoa(i), New(int64(i)))
	}

	for i := 0; i < 10; i++ {
		v := c.Get("key" + strconv.Itoa(i))
		if v.start != int64(i) {
			t.Error("Add failed")
		}
	}

	c.Add("key10", New(10))
	if ok := c.Get("key0"); ok != nil {
		t.Error("key0 should be removed")
	}

	fmt.Println(c.cache)
}
