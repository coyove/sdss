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
}

func cv(in rune) rune {
	s, ok := runeTable[in]
	if ok {
		return s
	}
	return in
}

func lemma(word string) string {
	return englishLemma.Lemma(word)
}

func removeAccents(s string) string {
	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	output, _, e := transform.String(t, s)
	if e != nil {
		return s
	}
	return output
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
