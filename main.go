package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/coyove/nj/bas"
	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/dal"
)

type Item struct {
	PartKey string
	SortKey string
	Fields  []bas.Value
}

func main() {
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
		if ln%10000 == 0 {
			fmt.Println("index", ln)
		}
		if ln > 100000 {
			break
		}
	}

	c := &dal.SearchCursor{
		Query:        "昆仑",
		Start:        clock.IdStr(),
		EndUnixMilli: clock.UnixMilli() - 500,
		Count:        5,
	}
	for !c.Exhausted {
		dal.SearchContent("a", c)
	}
}
