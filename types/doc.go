package types

import (
	"encoding/json"
	"fmt"

	"github.com/coyove/sdss/contrib/clock"
)

type Document struct {
	Id      string `json:"I"`
	Content string `json:"C"`
}

func (doc Document) PartKey() string {
	ts := doc.CreateTime()
	return fmt.Sprintf("doc%d", ts>>16)
}

func (doc *Document) MarshalBinary() []byte {
	buf, _ := json.Marshal(doc)
	return buf
}

func (doc *Document) CreateTime() int64 {
	ts, _ := clock.ParseIdStrUnix(doc.Id)
	return ts
}

func (doc *Document) String() string {
	return fmt.Sprintf("%d(%s): %q", doc.CreateTime(), doc.Id, doc.Content)
}

func StrHash(s string) uint32 {
	const offset32 = 2166136261
	const prime32 = 16777619
	var hash uint32 = offset32
	for i := 0; i < len(s); i++ {
		hash *= prime32
		hash ^= uint32(s[i])
	}
	return hash
}
