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

func (doc *Document) MarshalBinary() []byte {
	buf, _ := json.Marshal(doc)
	return buf
}

func (doc *Document) CreateTime() int64 {
	ts, _ := clock.ParseStrUnix(doc.Id)
	return ts
}

func (doc *Document) String() string {
	return fmt.Sprintf("%d(%s): %q", doc.CreateTime(), doc.Id, doc.Content)
}

type DocumentToken struct {
	Namespace string
	Id        string
	Token     string
	OutError  chan error
}

func StrHash(s string) uint32 {
	const offset32 = 2166136261
	const prime32 = 16777619
	var hash uint32 = offset32
	for i := range s {
		hash *= prime32
		hash ^= uint32(s[i])
	}
	return hash
}
