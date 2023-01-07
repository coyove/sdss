package main

import (
	"embed"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coyove/sdss/contrib/bitmap"
	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/dal"
	"github.com/coyove/sdss/types"
	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
)

//go:embed static/assets/*
var httpStaticAssets embed.FS

func HandleAssets(w http.ResponseWriter, r *types.Request) {
	p := "static/assets/" + strings.TrimPrefix(r.URL.Path, "/assets/")
	buf, _ := httpStaticAssets.ReadFile(p)
	w.Write(buf)
}

func HandlePostPage(w http.ResponseWriter, r *types.Request) {
	httpTemplates.ExecuteTemplate(w, "post.html", r)
}

func HandleTagAction(w http.ResponseWriter, r *types.Request) {
	q := r.URL.Query()
	id, err := strconv.ParseUint(q.Get("id"), 10, 64)
	action := q.Get("action")
	k := bitmap.Uint64Key(id)

	var old *types.Tag
	if action != "create" {
		old, err = dal.GetTag(id)
		if !old.Valid() || err != nil {
			logrus.Errorf("tag manage action %s, can't find %d: %v", action, id, err)
			writeJSON(w, "success", false, "code", "INTERNAL_ERROR")
			return
		}
	}

	switch action {
	case "update", "create":
		n := q.Get("text")
		h := buildBitmapHashes(n)
		if len(n) < 1 || len(n) > 100 || len(h) == 0 {
			writeJSON(w, "success", false, "code", "INVALID_CONTENT")
			return
		}
		var err error
		if action == "create" {
			err = dal.TagsStore.Update(func(tx *bbolt.Tx) error {
				bk, err := tx.CreateBucketIfNotExists([]byte("tags"))
				if err != nil {
					return err
				}
				last, _ := bk.Cursor().Last()
				lastId := bitmap.BytesKey(last).LowUint64()
				now := clock.UnixMilli()
				old = &types.Tag{
					Id:            lastId + 1,
					PendingReview: true,
					ReviewName:    n,
					Creator:       "root",
					CreateUnix:    now,
					UpdateUnix:    now,
				}
				k = bitmap.Uint64Key(old.Id)
				return dal.KSVUpsert(tx, "tags", dal.KSVFromTag(old))
			})
		} else {
			if old.PendingReview {
				writeJSON(w, "success", false, "code", "TAG_PENDING_REVIEW")
				return
			}
			old.PendingReview = true
			old.ReviewName = n
			old.UpdateUnix = clock.UnixMilli()
			err = dal.TagsStore.Update(func(tx *bbolt.Tx) error {
				return dal.KSVUpsert(tx, "tags", dal.KSVFromTag(old))
			})
		}
		if err != nil {
			logrus.Errorf("tag manage action %s %d: %v", action, id, err)
			writeJSON(w, "success", false, "code", "INTERNAL_ERROR")
			return
		}
		dal.TagsStore.Saver().AddAsync(k, h)
		writeJSON(w, "success", true, "tag", old)
		return
	case "delete":
		if err := dal.TagsStore.Update(func(tx *bbolt.Tx) error {
			return dal.KSVDelete(tx, "tags", k[:])
		}); err != nil {
			logrus.Errorf("tag manage action %s %d: %v", action, id, err)
			writeJSON(w, "success", false, "code", "INTERNAL_ERROR")
			return
		}
		writeJSON(w, "success", true)
		return
	case "approve", "reject":
		if !old.PendingReview {
			writeJSON(w, "success", false, "code", "INVALID_TAG_STATE")
			return
		}
		old.PendingReview = false
		if action == "approve" {
			old.Name = old.ReviewName
			old.Reviewer = "root2"
		}
		old.ReviewName = ""
		if err := dal.TagsStore.Update(func(tx *bbolt.Tx) error {
			if old.Name == "" && action == "reject" {
				return dal.KSVDelete(tx, "tags", k[:])
			}
			return dal.KSVUpsert(tx, "tags", dal.KSVFromTag(old))
		}); err != nil {
			logrus.Errorf("tag manage action %s %d: %v", action, id, err)
			writeJSON(w, "success", false, "code", "INTERNAL_ERROR")
			return
		}
		writeJSON(w, "success", true)
		return
	}
	writeJSON(w, "success", false, "code", "INVALID_ACTION")
}

func HandleTagManage(w http.ResponseWriter, r *types.Request) {
	p, _ := strconv.Atoi(r.URL.Query().Get("p"))
	if p < 1 {
		p = 1
	}
	sort, _ := strconv.Atoi(r.URL.Query().Get("sort"))
	if sort < -1 || sort > 1 {
		sort = 0
	}
	desc := r.URL.Query().Get("desc") == "1"

	locateId, _ := strconv.ParseUint(r.URL.Query().Get("id"), 10, 64)
	q := r.URL.Query().Get("q")

	var tags []*types.Tag
	var pages int
	if q != "" {
		sort, desc = -1, false
		tags, _ = collectSimple(q)
		if len(tags) > 1000 {
			tags = tags[:1000]
		}
	} else {
		var results []dal.KeySortValue
		dal.TagsStore.View(func(tx *bbolt.Tx) error {
			if locateId > 0 {
				k := bitmap.Uint64Key(locateId)
				results, p, pages = dal.KSVPagingLocateKey(tx, "tags", k[:], 100)
				sort = -1
				desc = false
			} else {
				results, pages = dal.KSVPaging(tx, "tags", sort, desc, p-1, 100)
			}
			return nil
		})

		tags = make([]*types.Tag, len(results))
		for i := range tags {
			tags[i] = types.UnmarshalTagBinary(results[i].Value)
		}
	}

	r.AddTemplateValue("tags", tags)
	r.AddTemplateValue("pages", pages)
	r.AddTemplateValue("page", p)
	r.AddTemplateValue("sort", sort)
	r.AddTemplateValue("desc", desc)
	httpTemplates.ExecuteTemplate(w, "tag_manage.html", r)
}

func HandleTagSearch(w http.ResponseWriter, r *types.Request) {
	start := time.Now()
	q := r.URL.Query().Get("q")
	n, _ := strconv.Atoi(r.URL.Query().Get("n"))
	if n <= 0 {
		n = 100
	} else if n > 100 {
		n = 100
	}

	tags, jms := collectSimple(q)
	results := []interface{}{}
	for i, tag := range tags {
		if i >= n {
			break
		}
		if tag.Name != "" {
			results = append(results, [2]interface{}{tag.Id, tag.Name})
		}
	}
	diff := time.Since(start)
	writeJSON(w,
		"tags", results,
		"elapsed", diff.Milliseconds(),
		"elapsed_us", diff.Microseconds(),
		"debug", fmt.Sprint(jms),
		"count", len(results),
	)
}
