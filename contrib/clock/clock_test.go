package clock

import (
	"fmt"
	"testing"
)

func TestClockId(t *testing.T) {
	fmt.Println(Unix())
	for i := 0; i < 10; i++ {
		fmt.Println(Id(), ParseUnixDeci(Id()), IdStr())
	}
	s := IdStr()
	fmt.Println(s)
	fmt.Println(ParseStrUnixDeciDeci(s))
}

func BenchmarkParseId(b *testing.B) {
	id := IdStr()
	for i := 0; i < b.N; i++ {
		_, ok := ParseStrUnixDeciDeci(id)
		if !ok {
			b.FailNow()
		}
	}
}
