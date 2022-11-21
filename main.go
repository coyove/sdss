package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/dal"
	"github.com/coyove/sdss/types"
)

func main() {
	runtime.GOMAXPROCS(2)
	types.LoadConfig("config.json")
	// dal.InitDB()

	if true {
		f, err := os.Open(os.Getenv("HOME") + "/Downloads/a.txt")
		// f, err := os.Open(os.Getenv("HOME") + "/dataset/dataset/full_dataset.csv")
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
			if ln <= 4 {
				continue
			}
			doc := &types.Document{
				Id:      clock.IdStr(),
				Content: line,
			}
			dal.IndexContent([]string{"a"}, doc)
			if ln%100 == 0 {
				fmt.Println("index", ln, doc.Id) // , doc.CreateTime(), line)
			}
			if ln > 3000 {
				break
			}
			time.Sleep(time.Millisecond * 50)
		}
	}

	c := &dal.SearchCursor{
		Query:   "学校学生",
		Exclude: "社团",
		Start:   clock.IdStr(),
		EndUnix: clock.UnixDeci() - 6000,
		Count:   5,
	}
	for !c.Exhausted {
		dal.SearchContent("a", c)
		break
	}

}
