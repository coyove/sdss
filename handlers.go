package main

import (
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/coyove/sdss/contrib/bitmap"
	"github.com/coyove/sdss/contrib/cursor"
	"github.com/coyove/sdss/contrib/ngram"
	"github.com/coyove/sdss/dal"
	"github.com/coyove/sdss/types"
	"go.etcd.io/bbolt"
)

//go:embed static/*
var httpStaticPages embed.FS

//go:embed static/assets/*
var httpStaticAssets embed.FS

var httpTemplates = template.Must(template.ParseFS(httpStaticPages, "static/*.html"))

func HandleAssets(w http.ResponseWriter, r *types.Request) {
	p := "static/assets/" + strings.TrimPrefix(r.URL.Path, "/assets/")
	buf, _ := httpStaticAssets.ReadFile(p)
	w.Write(buf)
}

func HandleTagSearchPage(w http.ResponseWriter, r *types.Request) {
	httpTemplates.ExecuteTemplate(w, "index.html", nil)
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

	h := ngram.SplitMore(q).Hashes()
	h2 := ngram.Split(q).Hashes()

	results := [][2]interface{}{}
	if len(h) == 0 {
		writeJSON(w, "tags", results, "count", 0)
		return
	}

	res, jms := dal.TagsStore.CollectSimple(cursor.New(), bitmap.Values{Major: h2, Exact: h}, 2000)

	var tags []*types.Tag
	dal.TagsStore.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket([]byte("tags"))
		if bk == nil {
			return nil
		}
		for _, kis := range res {
			tag := types.UnmarshalTagBinary(bk.Get(kis.Key[:]))
			if tag.Valid() {
				tags = append(tags, tag)
			}
		}
		return nil
	})

	sort.Slice(tags, func(i, j int) bool { return len(tags[i].Name) < len(tags[j].Name) })
	for i, tag := range tags {
		if i >= n {
			break
		}
		results = append(results, [2]interface{}{tag.Id, tag.Name})
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
