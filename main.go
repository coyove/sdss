package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/dal"
	"github.com/coyove/sdss/types"
)

func main() {
	runtime.GOMAXPROCS(2)
	types.LoadConfig("config.json")
	dal.InitDB()

	if false {
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
			doc := &types.Document{
				Id:      clock.IdStr(),
				Content: line,
			}
			dal.IndexContent([]string{"a"}, doc)
			if ln%1 == 0 {
				fmt.Println("index", ln, doc.Id, doc.CreateTime(), line)
			}
			if ln > 3 {
				break
			}
		}
	}

	c := &dal.SearchCursor{
		Query:   "学校朋友",
		Start:   clock.IdStr(),
		EndUnix: clock.Unix() - 6000,
		Count:   5,
	}
	for !c.Exhausted {
		dal.SearchContent("a", c)
	}
}
