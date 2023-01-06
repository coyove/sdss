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

	if *debugRebuild > 0 {
		rebuildData(*debugRebuild)
	}

	serve("/tag", HandleTagSearchPage)
	serve("/tag/search", HandleTagSearch)

	http.Handle("/static/", http.StripPrefix("/", http.FileServer(http.FS(httpStaticAssets))))

	logrus.Info("start serving")
	http.ListenAndServe(":8888", nil)
}
