package sdss

import "github.com/coyove/nj/bas"

type Item struct {
	PartKey string
	SortKey string
	Fields  []bas.Value
}
