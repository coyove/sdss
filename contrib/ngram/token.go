package ngram

import (
	"bytes"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

func walk(text string, f func(byte, string)) {
	text = strings.TrimSpace(text)
	for len(text) > 0 {
		idx := strings.IndexByte(text, ' ')
		var q string
		if idx == -1 {
			q, text = text, ""
		} else {
			q, text = text[:idx], text[idx+1:]
		}
		if len(q) == 0 {
			continue
		}
		switch q[0] {
		case '-', '+', '?':
			f(q[0], q[1:])
		default:
			f('?', q)
		}
	}
}

func Split(text string) (res map[string]float64) {
	res = map[string]float64{}

	tmpbuf := bytes.Buffer{}
	total := 0
	lastSplitText := ""

	splitter := func(v string) {
		if v == "" {
			return
		}

		r, _ := utf8.DecodeRuneInString(v)
		if lastSplitText != "" {
			lastr, _ := utf8.DecodeLastRuneInString(lastSplitText)
			if (lastr <= utf8.RuneSelf) != (r <= utf8.RuneSelf) {
				tmpbuf.Reset()
				tmpbuf.WriteRune(lastr)
				tmpbuf.WriteRune(r)
				res[strings.ToLower(tmpbuf.String())]++
				total++
			}
		}
		// fmt.Println(lastSplitText, v)
		lastSplitText = v

		if r < utf8.RuneSelf {
			if len(v) == 1 {
				return
			}
			res[strings.ToLower(v)]++
			total++
			return
		}

		lastr := utf8.RuneError
		runeCount, old := 0, v
		for len(v) > 0 {
			r, sz := utf8.DecodeRuneInString(v)
			v = v[sz:]

			if lastr != utf8.RuneError {
				tmpbuf.Reset()
				tmpbuf.WriteRune(lastr)
				tmpbuf.WriteRune(r)
				res[tmpbuf.String()]++
				total++
			}

			lastr = r
			runeCount++
		}

		if runeCount <= 4 {
			res[old]++
		}
	}

	lasti, i, lastr := 0, 0, utf8.RuneError
	for i < len(text) {
		r, sz := utf8.DecodeRuneInString(text[i:])
		if r == utf8.RuneError {
			goto BREAK
		}

		// fmt.Println(string(lastr), string(r), isdiff(lastr, r))
		if lastr != utf8.RuneError {
			isdiff := false
			if ac, bc := Continue(lastr), Continue(r); ac != bc {
				isdiff = true
			}
			if ac, bc := lastr <= utf8.RuneSelf, r <= utf8.RuneSelf; ac != bc {
				isdiff = true
			}
			if isdiff {
				splitter(text[lasti:i])
				lasti = i
			}
		}
		i += sz

		if Continue(r) {
			lastr = r
		} else {
			lastr = utf8.RuneError
			lasti = i
		}
	}
	splitter(text[lasti:])

BREAK:
	for k, v := range res {
		res[k] = v / float64(total)
	}
	return
}

type set func(rune) bool

func (a set) add(rt *unicode.RangeTable) set {
	b := in(rt)
	return func(r rune) bool { return a(r) || b(r) }
}

func (a set) sub(rt *unicode.RangeTable) set {
	b := in(rt)
	return func(r rune) bool { return a(r) && !b(r) }
}

func in(rt *unicode.RangeTable) set {
	return func(r rune) bool { return unicode.Is(rt, r) }
}

var id_continue = set(unicode.IsLetter).
	add(unicode.Nl).
	add(unicode.Other_ID_Start).
	sub(unicode.Pattern_Syntax).
	sub(unicode.Pattern_White_Space).
	add(unicode.Mn).
	add(unicode.Mc).
	add(unicode.Nd).
	add(unicode.Pc).
	add(unicode.Other_ID_Continue).
	sub(unicode.Pattern_Syntax).
	sub(unicode.Pattern_White_Space)

// Continue checks that the rune continues an identifier.
func Continue(r rune) bool {
	// id_continue(r) && NFKC(r) in "id_continue*"
	if !id_continue(r) {
		return false
	}
	for _, r := range norm.NFKC.String(string(r)) {
		if !id_continue(r) {
			return false
		}
	}
	return true
}
