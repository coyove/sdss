package clock

import (
	"fmt"
	"testing"
)

func TestClockId(t *testing.T) {
	fmt.Println(Unix())
	for i := 0; i < 10; i++ {
		fmt.Println(Id(), ParseIdUnixDeci(Id()), IdStr())
	}
	s := IdStr()
	fmt.Println(s)
	fmt.Println(ParseStrUnixDeci(s))
}

func BenchmarkParseId(b *testing.B) {
	id := IdStr()
	for i := 0; i < b.N; i++ {
		_, ok := ParseStrUnixDeci(id)
		if !ok {
			b.FailNow()
		}
	}
}
