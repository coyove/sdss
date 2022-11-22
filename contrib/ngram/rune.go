package ngram

import (
	"bytes"
	_ "embed"
	"unicode/utf8"

	"github.com/aaaton/golem/v4"
	"github.com/aaaton/golem/v4/dicts/en"
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
