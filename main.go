package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/dal"
)

func main() {
	runtime.GOMAXPROCS(2)
	// types.LoadConfig("config.json")
	// dal.InitDB()

	if true {
		f, err := os.Open(os.Getenv("HOME") + "/Downloads/a.txt")
		if err != nil {
			panic(err)
		}
		defer f.Close()
		rd := bufio.NewReader(f)
		for ln := 0; ; ln++ {
			line, err := rd.ReadString('\n')
			if err != nil {
				break
			}
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				continue
			}
			dal.IndexContent([]string{"a"}, clock.IdStr(), line)
			if ln%1000 == 0 {
				fmt.Println("index", ln)
			}
			if ln > 10000 {
				break
			}
		}
	}

	c := &dal.SearchCursor{
		Query:   "华夏",
		Start:   clock.IdStr(),
		EndUnix: clock.Unix() - 60,
		Count:   5,
	}
	for !c.Exhausted {
		dal.SearchContent("a", c)
	}
}
