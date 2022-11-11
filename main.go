package main

import (
	"github.com/coyove/nj/bas"
	"github.com/coyove/sdss/contrib/clock"
	"github.com/coyove/sdss/contrib/ngram"
	"github.com/coyove/sdss/dal"
)

type Item struct {
	PartKey string
	SortKey string
	Fields  []bas.Value
}

func main() {
	dal.IndexContent([]string{"a", "b"}, clock.IdStr(), ngram.Split("abc"))
}
