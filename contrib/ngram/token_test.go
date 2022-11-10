package ngram

import (
	"fmt"
	"testing"
)

func TestNGram(t *testing.T) {
	fmt.Println(Split(`		女朋友要求戒指 5-7w 预算过分么。
r00t7		通勤 50 分钟，费用 7.2 元有必要买一辆小电驴吗`))
}
