package dal

import (
	"fmt"
	"testing"

	"github.com/coyove/sdss/contrib/clock"
)

func TestBitmap(t *testing.T) {
	n := genBitmapBlockName("a", "b", clock.Unix(), "")
	fmt.Println(n)
	fmt.Println(parseBitmapBlockName(n))
	fmt.Println(parseBitmapBlockName(n + ":server"))
}
