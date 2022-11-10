package clock

import (
	"fmt"
	"testing"
)

func TestClockId(t *testing.T) {
	for i := 0; i < 10; i++ {
		fmt.Println(Id(), ParseTime(Id()), IdStr())
	}
	s := IdStr()
	fmt.Println(s)
	fmt.Println(ParseTimeStr(s))
}

func BenchmarkParseId(b *testing.B) {
	id := IdStr()
	for i := 0; i < b.N; i++ {
		_, ok := ParseTimeStr(id)
		if !ok {
			b.FailNow()
		}
	}
}
