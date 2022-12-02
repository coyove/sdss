package ngram

import (
	"bytes"
	"fmt"
	"unicode"
	"unicode/utf8"

	"github.com/coyove/sdss/types"
)

type Token struct {
	Name string  `json:"name"`
	Raw  string  `json:"raw"`
	Freq float64 `json:"freq"`
	// Quoted bool    `json:"quoted"`
}

func (tok Token) String() string {
	s := tok.Name
	if s != tok.Raw {
		s = "<" + s + "," + tok.Raw + ">"
	}
	// if tok.Quoted {
	// 	return fmt.Sprintf("%q(%.3f)", s, tok.Freq)
	// }
	return fmt.Sprintf("%s(%.3f)", s, tok.Freq)
}

func SplitHash(text string) (res map[string]Token, qs []uint32) {
	if text == "" {
		return
	}
	res = Split(text)
	for k := range res {
		qs = append(qs, types.StrHash(k))
	}
	return
}

func Split(text string) (res map[string]Token) {
	// text = removeAccents(text)
	res = map[string]Token{}
	sp := splitter{
		freq: map[string]float64{},
	}

	prevStart, prevRune, prevRuneNormalized := 0, utf8.RuneError, utf8.RuneError
	// inQuote := false

	var i int
	for i < len(text) {
		r, sz := utf8.DecodeRuneInString(text[i:])
		if r == utf8.RuneError {
			goto BREAK
		}

		// if inQuote {
		// 	if r == '"' {
		// 		sp.do(text[prevStart:i], res, true)
		// 		prevStart = i + sz
		// 		inQuote = false
		// 	}
		// 	i += sz
		// 	continue
		// }

		// fmt.Println(string(lastr), string(r), isdiff(lastr, r))
		if prevRune != utf8.RuneError {
			isdiff := false
			if isContinue(prevRune) != isContinue(r) {
				isdiff = true
			}
			if (prevRuneNormalized <= utf8.RuneSelf) != (normal(r) <= utf8.RuneSelf) {
				isdiff = true
			}
			if isdiff {
				sp.do(text[prevStart:i], res, false)
				prevStart = i
			}
		}
		i += sz

		if isContinue(r) {
			prevRune = r
			prevRuneNormalized = normal(r)
		} else {
			prevRune = utf8.RuneError
			prevStart = i
			// inQuote = r == '"'
		}
	}
	sp.do(text[prevStart:], res, false)

BREAK:
	for k, v := range sp.freq {
		tok := res[k]
		tok.Freq = v / float64(sp.total)
		res[k] = tok
		// if tok.Quoted {
		// 	for k0, v0 := range Split(res[k].Name) {
		// 		res[k0] = v0
		// 	}
		// }
	}
	return
}

type splitter struct {
	tmpbuf        bytes.Buffer
	total         int
	lastSplitText string
	freq          map[string]float64
}

func (s *splitter) do(v string, res map[string]Token, inQuote bool) {
	if v == "" {
		return
	}

	r, _ := utf8.DecodeRuneInString(v)
	if s.lastSplitText != "" {
		lastr, _ := utf8.DecodeLastRuneInString(s.lastSplitText)
		if (lastr <= utf8.RuneSelf) != (r <= utf8.RuneSelf) {
			s.tmpbuf.Reset()
			s.tmpbuf.WriteRune(unicode.ToLower(cv(lastr)))
			s.tmpbuf.WriteRune(unicode.ToLower(cv(r)))
			n := s.tmpbuf.Len()
			s.tmpbuf.WriteRune(lastr)
			s.tmpbuf.WriteRune(r)
			x := s.tmpbuf.String()

			s.freq[x[:n]]++
			res[x[:n]] = Token{Name: x[:n], Raw: x[n:]} // , Quoted: inQuote}
			s.total++
		}
	}
	// fmt.Println(lastSplitText, v)
	s.lastSplitText = v

	if normal(r) < utf8.RuneSelf {
		if len(v) == 1 {
			return
		}

		x := lemma(v)
		if isCodeString(x) {
			for _, x := range trigram(x) {
				s.freq[x]++
				res[x] = Token{Name: x, Raw: x}
			}
		} else {
			s.freq[x]++
			res[x] = Token{Name: x, Raw: v} //, Quoted: inQuote}
		}
		s.total++
		return
	}

	lastr := utf8.RuneError
	runeCount := 0
	for len(v) > 0 {
		r, sz := utf8.DecodeRuneInString(v)
		v = v[sz:]

		if lastr != utf8.RuneError {
			s.tmpbuf.Reset()
			s.tmpbuf.WriteRune(cv(lastr))
			s.tmpbuf.WriteRune(cv(r))
			n := s.tmpbuf.Len()
			s.tmpbuf.WriteRune(lastr)
			s.tmpbuf.WriteRune(r)
			x := s.tmpbuf.String()

			s.freq[x[:n]]++
			res[x[:n]] = Token{Name: x[:n], Raw: x[n:]} //, Quoted: inQuote}
			s.total++
		}

		lastr = r
		runeCount++
	}
}
