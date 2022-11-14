package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/dal"
	"github.com/coyove/sdss/types"
)

func main() {
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
			fmt.Println(ln, dal.IndexContent([]string{"a"}, clock.IdStr(), line))
			if ln%1 == 0 {
				fmt.Println("index", ln)
			}
			if ln > 10000 {
				break
			}
		}
	}

	c := &dal.SearchCursor{
		Query:   "我对上帝",
		Start:   clock.IdStr(),
		EndUnix: clock.Unix() - 60,
		Count:   5,
	}
	for !c.Exhausted {
		dal.SearchContent("a", c)
	}
}
