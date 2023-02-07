package ngram

import (
	"bytes"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Token struct {
	Name string  `json:"name"`
	Raw  string  `json:"raw"`
	Freq float64 `json:"freq"`
}

func (tok Token) String() string {
	s := tok.Name
	if s != tok.Raw {
		s = "<" + s + "," + tok.Raw + ">"
	}
	return fmt.Sprintf("%s(%.3f)", s, tok.Freq)
}

type Results map[string]Token

func (r Results) Contains(r2 Results) bool {
	for k := range r2 {
		if _, ok := r[k]; !ok {
			return false
		}
	}
	return true
}

func (r Results) String() string {
	var lines [][2]string
	var max1 int
	for k, v := range r {
		y := ""
		for _, r := range k {
			if r < 128 {
				y += fmt.Sprintf("%c ", r)
			} else if r < 65536 {
				y += fmt.Sprintf("\\u%04X ", r)
			} else {
				y += fmt.Sprintf("\\U%08X ", r)
			}
		}
		z := v.String()
		lines = append(lines, [2]string{y, z})
		if len(y) > max1 {
			max1 = len(y)
		}
	}
	if max1 > 50 {
		max1 = 50
	}

	buf := &bytes.Buffer{}
	for _, line := range lines {
		buf.WriteString(line[0])
		for i := 0; i < max1-len(line[0]); i++ {
			buf.WriteByte(' ')
		}
		buf.WriteString(line[1])
		buf.WriteString("\n")
	}
	return buf.String()
}

func (r Results) Hashes() (qs []uint64) {
	for k := range r {
		qs = append(qs, StrHash(k))
	}
	return
}

func Split(text string) (res Results) {
	return doSplit(text, false)
}

func SplitMore(text string) (res Results) {
	return doSplit(text, true)
}

func doSplit(text string, more bool) (res Results) {
	// text = removeAccents(text)
	res = map[string]Token{}
	sp := splitter{
		more: more,
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

		if eps, ok := emojiTree[r]; ok {
			found := false
			for _, ep := range eps {
				if strings.HasPrefix(text[i:], ep) {
					sp.freq[ep]++
					sp.total++
					if prevRune != utf8.RuneError {
						sp.do(text[prevStart:i], res, false)
						prevRune = utf8.RuneError
					}
					i += len(ep)
					prevStart = i
					found = true
					break
				}
			}
			if found {
				continue
			}
		}

		// fmt.Println(string(lastr), string(r), isdiff(lastr, r))
		if prevRune != utf8.RuneError {
			isdiff := false
			if isContinue(prevRune) != isContinue(r) {
				isdiff = true
			}
			if (prevRuneNormalized <= utf8.RuneSelf) != (Normalize(r) <= utf8.RuneSelf) {
				isdiff = true
			}
			// fmt.Println(text[prevStart:i], string(prevRuneNormalized), string(prevRune))
			if isdiff {
				sp.do(text[prevStart:i], res, false)
				prevStart = i
			}
		}
		i += sz

		if isContinue(r) {
			prevRune = r
			prevRuneNormalized = Normalize(r)
		} else {
			if r > 65535 || (more && !unicode.IsSpace(r)) {
				t := text[prevStart:i]
				sp.freq[t]++
				sp.total++
			}
			prevRune = utf8.RuneError
			prevStart = i
			// inQuote = r == '"'
		}
	}
	sp.do(text[prevStart:], res, false)

BREAK:
	for k, v := range sp.freq {
		tok := res[k]
		if tok.Name == "" {
			tok.Name, tok.Raw = k, k
		}
		tok.Freq = v / float64(sp.total)
		res[k] = tok
	}
	return
}

type splitter struct {
	tmpbuf        bytes.Buffer
	total         int
	more          bool
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
			lastrn := Normalize(lastr)
			rn := Normalize(r)
			if (lastrn <= utf8.RuneSelf) != (rn <= utf8.RuneSelf) { // test again
				s.tmpbuf.Reset()
				s.tmpbuf.WriteRune(unicode.ToLower(lastrn))
				s.tmpbuf.WriteRune(unicode.ToLower(rn))
				n := s.tmpbuf.Len()
				s.tmpbuf.WriteRune(lastr)
				s.tmpbuf.WriteRune(r)
				x := s.tmpbuf.String()

				s.freq[x[:n]]++
				res[x[:n]] = Token{Name: x[:n], Raw: x[n:]} // , Quoted: inQuote}
				s.total++
			}
		}
	}
	// fmt.Println(lastSplitText, v)
	s.lastSplitText = v

	if Normalize(r) < utf8.RuneSelf || unicode.IsLower(r) || unicode.IsUpper(r) {
		if len(v) == 1 && !s.more {
			return
		}

		x := v
		if len(v) > 3 {
			x = lemma(v)
		} else {
			x = strings.ToLower(removeAccents(v))
		}
		if s.more {
			for _, g := range trigram(x) {
				s.freq[g]++
				s.total++
			}
		} else {
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
		}
		return
	}

	lastr := utf8.RuneError
	runeCount := 0
	for len(v) > 0 {
		r, sz := utf8.DecodeRuneInString(v)
		v = v[sz:]

		if s.more {
			s.tmpbuf.Reset()
			s.tmpbuf.WriteRune(cv(r))
			n := s.tmpbuf.Len()
			s.tmpbuf.WriteRune(r)
			x := s.tmpbuf.String()

			s.freq[x[:n]]++
			res[x[:n]] = Token{Name: x[:n], Raw: x[n:]}
		} else {
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
		}

		lastr = r
		runeCount++
	}
}

func StrHash(s string) uint64 {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211
	var hash uint64 = offset64
	for i := 0; i < len(s); i++ {
		hash *= prime64
		hash ^= uint64(s[i])
	}
	return uint64(hash)
}
