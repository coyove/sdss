package ngram

import (
	"bytes"
	"fmt"
	"unicode"
	"unicode/utf8"
)

type Token struct {
	Name   string
	Freq   float64
	Quoted bool
}

func (tok Token) String() string {
	if tok.Quoted {
		return fmt.Sprintf("%q(%.3f)", tok.Name, tok.Freq)
	}
	return fmt.Sprintf("%s(%.3f)", tok.Name, tok.Freq)
}

func Split(text string) (res map[string]Token) {
	text = removeAccents(text)
	res = map[string]Token{}
	sp := splitter{
		freq: map[string]float64{},
	}

	prevStart, prevRune := 0, utf8.RuneError
	inQuote := false

	var i int
	for i < len(text) {
		r, sz := utf8.DecodeRuneInString(text[i:])
		if r == utf8.RuneError {
			goto BREAK
		}

		if inQuote {
			if r == '"' {
				sp.do(text[prevStart:i], res, true)
				prevStart = i + sz
				inQuote = false
			}
			i += sz
			continue
		}

		// fmt.Println(string(lastr), string(r), isdiff(lastr, r))
		if prevRune != utf8.RuneError {
			isdiff := false
			if ac, bc := isContinue(prevRune), isContinue(r); ac != bc {
				isdiff = true
			}
			if ac, bc := prevRune <= utf8.RuneSelf, r <= utf8.RuneSelf; ac != bc {
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
		} else {
			prevRune = utf8.RuneError
			prevStart = i
			inQuote = r == '"'
		}
	}
	sp.do(text[prevStart:], res, false)

BREAK:
	for k, v := range sp.freq {
		res[k] = Token{
			Name:   k,
			Freq:   v / float64(sp.total),
			Quoted: res[k].Quoted,
		}
		if res[k].Quoted {
			for k0, v0 := range Split(res[k].Name) {
				res[k0] = v0
			}
		}
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

	tok := Token{Quoted: inQuote}

	r, _ := utf8.DecodeRuneInString(v)
	if s.lastSplitText != "" {
		lastr, _ := utf8.DecodeLastRuneInString(s.lastSplitText)
		if (lastr <= utf8.RuneSelf) != (r <= utf8.RuneSelf) {
			s.tmpbuf.Reset()
			s.tmpbuf.WriteRune(unicode.ToLower(cv(lastr)))
			s.tmpbuf.WriteRune(unicode.ToLower(cv(r)))
			x := s.tmpbuf.String()

			s.freq[x]++
			res[x] = tok
			s.total++
		}
	}
	// fmt.Println(lastSplitText, v)
	s.lastSplitText = v

	if r < utf8.RuneSelf {
		if len(v) == 1 {
			return
		}
		x := lemma(v)
		s.freq[x]++
		res[x] = tok
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
			x := s.tmpbuf.String()
			s.freq[x]++
			res[x] = tok
			s.total++
		}

		lastr = r
		runeCount++
	}
}
