package dal

import (
	"fmt"

	"github.com/coyove/sdss/contrib/bitmap"
	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/types"
	"go.etcd.io/bbolt"
)

func BatchGetTags(v interface{}) (tags []*types.Tag, err error) {
	var ids []bitmap.Key
	switch v := v.(type) {
	case []bitmap.Key:
		ids = v
	case []uint64:
		for _, v := range v {
			ids = append(ids, bitmap.Uint64Key(v))
		}
	default:
		panic(v)
	}
	err = TagsStore.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("tags"))
		if bk == nil {
			return nil
		}
		for _, kis := range ids {
			tag := types.UnmarshalTagBinary(bk.Get(kis[:]))
			if tag.Valid() {
				tags = append(tags, tag)
			}
		}
		return nil
	})
	return
}

func GetTagRecord(id bitmap.Key) (*types.TagRecord, error) {
	var t *types.TagRecord
	err := TagsStore.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("tags_history"))
		if bk == nil {
			return nil
		}
		t = types.UnmarshalTagRecordBinary(bk.Get(id[:]))
		return nil
	})
	return t, err
}

func GetTag(id uint64) (*types.Tag, error) {
	var t *types.Tag
	err := TagsStore.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("tags"))
		if bk == nil {
			return nil
		}
		k := bitmap.Uint64Key(id)
		t = types.UnmarshalTagBinary(bk.Get(k[:]))
		return nil
	})
	return t, err
}

func ProcessTagParentChanges(tx *bbolt.Tx, tag *types.Tag, old, new []uint64) error {
	k := bitmap.Uint64Key(tag.Id)
	for _, o := range old {
		if err := KSVDelete(tx, fmt.Sprintf("tags_children_%d", o), k[:]); err != nil {
			return err
		}
	}
	now := clock.UnixMilli()
	for _, n := range new {
		if err := KSVUpsert(tx, fmt.Sprintf("tags_children_%d", n), KeySortValue{
			Key:   k[:],
			Sort0: uint64(now),
			Sort1: []byte(tag.Name),
			Value: nil,
		}); err != nil {
			return err
		}
	}
	return nil
}

func ProcessTagHistory(tagId uint64, user, action string, old, new string) error {
	return TagsStore.Update(func(tx *bbolt.Tx) error {
		tr := &types.TagRecord{
			Id:         clock.Id(),
			CreateUnix: clock.UnixMilli(),
			Action:     action,
			From:       old,
			To:         new,
			Modifier:   user,
		}
		k := bitmap.Uint64Key(tr.Id)
		KSVUpsert(tx, "tags_history", KeySortValue{
			Key:    k[:],
			Value:  tr.MarshalBinary(),
			NoSort: true,
		})
		return KSVUpsert(tx, fmt.Sprintf("tags_history_%d", tagId), KeySortValue{
			Key:    k[:],
			NoSort: true,
		})
	})
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
