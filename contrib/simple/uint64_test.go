package simple

import (
	"fmt"
	"testing"
)

func TestDedup(t *testing.T) {
	a := []uint64{1, 3, 2, 2}
	a = Uint64.Dedup(a)
	fmt.Println(a)
}
