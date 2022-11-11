package clock

import (
	"fmt"
	"testing"
)

func TestClockId(t *testing.T) {
	fmt.Println(UnixMilli())
	for i := 0; i < 10; i++ {
		fmt.Println(Id(), ParseUnixMilli(Id()), IdStr())
	}
	s := IdStr()
	fmt.Println(s)
	fmt.Println(ParseStrUnixMilli(s))
}

func BenchmarkParseId(b *testing.B) {
	id := IdStr()
	for i := 0; i < b.N; i++ {
		_, ok := ParseStrUnixMilli(id)
		if !ok {
			b.FailNow()
		}
	}
}
