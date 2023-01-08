package main

import (
	"flag"
	"net/http"

	"github.com/coyove/sdss/dal"
	"github.com/coyove/sdss/types"
	"github.com/sirupsen/logrus"
)

var debugRebuild = flag.Int("debug-rebuild", 0, "")

func main() {
	flag.Parse()

	types.LoadConfig("config.json")
	dal.InitDB()

	// dal.TagsStore.View(func(tx *bbolt.Tx) error {
	// 	fmt.Println(dal.KSVPaging(tx, "tags", 1, true, 2000, 10))
	// 	return nil
	// })
	// return

	if *debugRebuild > 0 {
		rebuildData(*debugRebuild)
	}

	serve("/post", HandlePostPage)
	serve("/tag/search", HandleTagSearch)
	serve("/tag/manage", HandleTagManage)
	serve("/tag/history", HandleTagHistory)
	serve("/tag/manage/action", HandleTagAction)

	http.Handle("/static/", http.StripPrefix("/", http.FileServer(http.FS(httpStaticAssets))))

	logrus.Info("start serving")
	http.ListenAndServe(":8888", nil)
}
