package ngram

import (
	"encoding/base64"
	"fmt"
	"testing"
)

func TestNGram(t *testing.T) {
	fmt.Println(isCodeString("AB"), isCodeString("Unsaved"), isCodeString(base64.URLEncoding.EncodeToString([]byte("base64"))))

	q := `		女朋友要求戒指 5-7w 预算😋❤️🥺过分么。
r00t7		child napıyor children通勤 50 分钟，费用 7.2 元有必要买一辆小电驴吗 12憂鬱台灣烏龜
15996301234 şile
quần quật 18 ếng 1 ngày 
𝘮𝘶𝘳𝘪𝘦𝘯𝘥𝘰 𝞉 600 جزء من احتفلتي مع اصدقائي`

	for k, v := range Split(q + "\U0001F1F7\U0001F1FA russia \u2764\ufe0f a") {
		fmt.Println(k, []byte(k), v)
	}
	return

	fmt.Println(trigram(q))
	fmt.Println("===")
	for _, v := range Split(`Random selfies #randomppic #lits"match.;newbie" set @abc.def #tag事实🤔的s`) {
		fmt.Println(v)
	}
}
