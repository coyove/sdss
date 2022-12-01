package ngram

import (
	"fmt"
	"testing"
)

func TestNGram(t *testing.T) {
	q := `		女朋友要求戒指 5-7w 预算过分么。
r00t7		child children通勤 50 分钟，费用 7.2 元有必要买一辆小电驴吗 12憂鬱台灣烏龜
15996301234
quần quật 18 ếng 1 ngày`
	fmt.Println(trigram(q))

	for k, v := range Split(q) {
		fmt.Println(k, v)
	}

	fmt.Println("===")
	for _, v := range Split(`Random selfies #randomppic #lits"match.;newbie" set @abc.def #tag事实🤔的s`) {
		fmt.Println(v)
	}
}
