package main

import (
	"embed"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/coyove/sdss/contrib/bitmap"
	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/contrib/ngram"
	"github.com/coyove/sdss/dal"
	"github.com/coyove/sdss/types"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
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
		dal.LockKey(id)
		defer dal.UnlockKey(id)

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
		var parentTags []uint64
		if pt := q.Get("parents"); pt != "" {
			gjson.Parse(pt).ForEach(func(key, value gjson.Result) bool {
				parentTags = append(parentTags, key.Uint())
				return true
			})
		}

		var err error
		if action == "create" {
			err = dal.TagsStore.Update(func(tx *bbolt.Tx) error {
				now := clock.UnixMilli()
				old = &types.Tag{
					Id:            clock.Id(),
					PendingReview: true,
					ReviewName:    n,
					Creator:       "root",
					ParentIds:     parentTags,
					CreateUnix:    now,
					UpdateUnix:    now,
				}
				k = bitmap.Uint64Key(old.Id)
				dal.ProcessTagParentChanges(tx, old, nil, parentTags)
				return dal.KSVUpsert(tx, "tags", dal.KSVFromTag(old))
			})
		} else {
			if old.PendingReview {
				writeJSON(w, "success", false, "code", "TAG_PENDING_REVIEW")
				return
			}
			err = dal.TagsStore.Update(func(tx *bbolt.Tx) error {
				dal.ProcessTagParentChanges(tx, old, old.ParentIds, parentTags)
				old.ParentIds = parentTags
				old.PendingReview = true
				old.ReviewName = n
				old.Modifier = "mod"
				old.UpdateUnix = clock.UnixMilli()
				return dal.KSVUpsert(tx, "tags", dal.KSVFromTag(old))
			})
		}
		if err != nil {
			logrus.Errorf("tag manage action %s %d: %v", action, id, err)
			writeJSON(w, "success", false, "code", "INTERNAL_ERROR")
			return
		}
		if old.ReviewName != old.Name {
			dal.TagsStore.Saver().AddAsync(k, h)
		}
		writeJSON(w, "success", true, "tag", old)
	case "delete":
		if err := dal.TagsStore.Update(func(tx *bbolt.Tx) error {
			return dal.KSVDelete(tx, "tags", k[:])
		}); err != nil {
			logrus.Errorf("tag manage action %s %d: %v", action, id, err)
			writeJSON(w, "success", false, "code", "INTERNAL_ERROR")
			return
		}
		writeJSON(w, "success", true)
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
	case "lock", "unlock":
		old.Lock = action == "lock"
		old.UpdateUnix = clock.UnixMilli()
		if err := dal.TagsStore.Update(func(tx *bbolt.Tx) error {
			return dal.KSVUpsert(tx, "tags", dal.KSVFromTag(old))
		}); err != nil {
			logrus.Errorf("tag manage action %s %d: %v", action, id, err)
			writeJSON(w, "success", false, "code", "INTERNAL_ERROR")
			return
		}
		writeJSON(w, "success", true)
	default:
		writeJSON(w, "success", false, "code", "INVALID_ACTION")
	}
}

func HandleTagManage(w http.ResponseWriter, r *types.Request) {
	p, st, desc, pageSize := getPagingArgs(r)
	q := r.URL.Query().Get("q")
	pid, _ := strconv.ParseUint(r.URL.Query().Get("pid"), 10, 64)

	var tags []*types.Tag
	var pages int
	if q != "" {
		st, desc = -1, false
		ids, _ := collectSimple(q)
		tags, _ = dal.BatchGetTags(ids)
		sort.Slice(tags, func(i, j int) bool { return len(tags[i].Name) < len(tags[j].Name) })
		tags = tags[:imin(1000, len(tags))]
	} else {
		var results []dal.KeySortValue
		if pid > 0 {
			results, pages = dal.KSVPaging(nil, fmt.Sprintf("tags_children_%d", pid), st, desc, p-1, pageSize)
			ids := make([]bitmap.Key, len(results))
			for i := range ids {
				ids[i] = bitmap.BytesKey(results[i].Key)
			}
			tags, _ = dal.BatchGetTags(ids)
			ptag, _ := dal.GetTag(pid)
			r.AddTemplateValue("ptag", ptag)
		} else {
			results, pages = dal.KSVPaging(nil, "tags", st, desc, p-1, pageSize)
			tags = make([]*types.Tag, len(results))
			for i := range tags {
				tags[i] = types.UnmarshalTagBinary(results[i].Value)
			}
		}
	}

	if editTagID, _ := strconv.Atoi(r.URL.Query().Get("edittagid")); editTagID > 0 {
		found := false
		for _, t := range tags {
			found = found || t.Id == uint64(editTagID)
		}
		if !found {
			if tag, _ := dal.GetTag(uint64(editTagID)); tag.Valid() {
				tags = append(tags, tag)
			}
		}
	}

	r.AddTemplateValue("pid", pid)
	r.AddTemplateValue("tags", tags)
	r.AddTemplateValue("pages", pages)
	r.AddTemplateValue("page", p)
	r.AddTemplateValue("sort", st)
	r.AddTemplateValue("desc", desc)
	httpTemplates.ExecuteTemplate(w, "tag_manage.html", r)
}

func HandleTagSearch(w http.ResponseWriter, r *types.Request) {
	start := time.Now()
	q := r.URL.Query().Get("q")
	n, _ := strconv.Atoi(r.URL.Query().Get("n"))
	n = imin(100, n)
	n = imax(1, n)

	var ids []bitmap.Key
	var jms []bitmap.JoinMetrics

	if tagIDs := r.URL.Query().Get("ids"); tagIDs != "" {
		for _, p := range strings.Split(tagIDs, ",") {
			id, _ := strconv.ParseUint(p, 10, 64)
			ids = append(ids, bitmap.Uint64Key(id))
		}
	} else {
		ids, jms = collectSimple(q)
	}

	tags, _ := dal.BatchGetTags(ids)
	sort.Slice(tags, func(i, j int) bool { return len(tags[i].Name) < len(tags[j].Name) })

	results := []interface{}{}
	h := ngram.SplitMore(q)
	for i, tag := range tags {
		if i >= n {
			break
		}
		if tag.Name != "" {
			if len(h) == 0 || ngram.SplitMore(tag.Name).Contains(h) {
				results = append(results, [3]interface{}{
					tag.Id,
					tag.Name,
					tag.ParentIds,
				})
			}
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
