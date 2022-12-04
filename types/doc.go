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

func StrHash(s string) uint64 {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211
	var hash uint64 = offset64
	for i := 0; i < len(s); i++ {
		hash *= prime64
		hash ^= uint64(s[i])
	}
	return uint64(hash)
}
