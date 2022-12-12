package ngram

import (
	"bytes"
	_ "embed"
	"unicode/utf8"

	"github.com/aaaton/golem/v4"
	"github.com/aaaton/golem/v4/dicts/en"

	"unicode"

	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

//go:embed TSCharacters.txt
var rawDictBuf []byte

var runeTable = map[rune]rune{}

var englishLemma *golem.Lemmatizer

func init() {
	for {
		idx := bytes.IndexByte(rawDictBuf, '\n')

		var line []byte
		if idx > 0 {
			line = rawDictBuf[:idx]
			rawDictBuf = rawDictBuf[idx+1:]
		} else {
			line = rawDictBuf
		}

		if len(line) == 0 {
			break
		}

		sep := bytes.IndexByte(line, '\t')
		a, _ := utf8.DecodeRune(line[:sep])
		b, _ := utf8.DecodeRune(line[sep+1:])
		runeTable[a] = b
	}

	englishLemma, _ = golem.New(en.New())

	runeTable['\u0131'] = 'i'
}

func cv(in rune) rune {
	s, ok := runeTable[in]
	if ok {
		return s
	}
	return in
}

func lemma(word string) string {
	return englishLemma.Lemma(removeAccents(word))
}

func removeAccents(s string) string {
	var accent = transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	output, _, e := transform.String(accent, s)
	if e != nil {
		return s
	}
	return output
}

func normal(r rune) rune {
	var accent = transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	var tmp [32]byte
	n := utf8.EncodeRune(tmp[:], r)
	output, _, e := transform.Append(accent, tmp[16:16], tmp[:n])
	if e != nil {
		return r
	}
	nr, _ := utf8.DecodeRune(output)
	return cv(nr)
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

// isContinue checks that the rune continues an identifier.
func isContinue(r rune) bool {
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

func isCodeString(v string) bool {
	// Hex string
	for _, b := range v {
		if ('0' <= b && b <= '9') || ('a' <= b && b <= 'f') || ('A' <= b && b <= 'F') {
		} else {
			goto BASE64
		}
	}
	return true

	// Base64 string
BASE64:
	ups := 0
	for _, b := range v {
		if 'A' <= b && b <= 'Z' {
			ups++
		}
	}
	if len(v) >= 6 && ups >= len(v)/3 {
		// There are approximately equal-number of upper letters and lower letters
		// in a base64 string
		return true
	}
	return false
}

func trigram(v string) (res []string) {
	orig := v
	idx := [3]int{0, 0, 0}
	x := 0
	for i := 1; len(v) > 0; i++ {
		r, sz := utf8.DecodeRuneInString(v)
		if r == utf8.RuneError {
			break
		}
		if i >= 3 {
			res = append(res, orig[idx[(i-3)%3]:idx[(i-1)%3]+sz])
		}
		x += sz
		idx[i%3] = x
		v = v[sz:]
	}
	if len(res) == 0 {
		res = append(res, orig)
	}
	return
}
