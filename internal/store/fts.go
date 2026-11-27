package store

import (
	"strings"
	"unicode"
)

// TokenizeCJK converts mixed CJK + ASCII text into space-delimited tokens for SQLite FTS5 unicode61.
// - ASCII alphanumeric/identifier runs are preserved intact (lowercased).
// - CJK runs emit overlapping bigrams, plus unigram for single-character CJK runs.
// Zero extra dictionary RAM, zero OOV misses on slang/nicknames/tickers.
func TokenizeCJK(input string) string {
	if input == "" {
		return ""
	}
	var tokens []string
	var asciiBuf strings.Builder
	var cjkRun []rune

	flushASCII := func() {
		if asciiBuf.Len() > 0 {
			tokens = append(tokens, strings.ToLower(asciiBuf.String()))
			asciiBuf.Reset()
		}
	}
	flushCJK := func() {
		n := len(cjkRun)
		if n == 0 {
			return
		}
		if n == 1 {
			tokens = append(tokens, string(cjkRun[0]))
		} else {
			for i := 0; i < n-1; i++ {
				tokens = append(tokens, string(cjkRun[i:i+2]))
			}
		}
		cjkRun = cjkRun[:0]
	}

	for _, r := range input {
		switch {
		case isCJK(r):
			flushASCII()
			cjkRun = append(cjkRun, r)
		case isASCIIToken(r):
			flushCJK()
			asciiBuf.WriteRune(r)
		default:
			flushASCII()
			flushCJK()
		}
	}
	flushASCII()
	flushCJK()
	return strings.Join(tokens, " ")
}

// BuildFTSQuery builds a safe SQLite FTS5 MATCH expression from user query text.
// Uses quoted token terms joined by OR (or AND if requireAll is true) to prevent FTS5 syntax errors.
func BuildFTSQuery(query string, requireAll bool) string {
	toks := strings.Fields(TokenizeCJK(query))
	if len(toks) == 0 {
		return ""
	}
	seen := make(map[string]struct{}, len(toks))
	quoted := make([]string, 0, len(toks))
	for _, t := range toks {
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		esc := strings.ReplaceAll(t, `"`, `""`)
		quoted = append(quoted, `"`+esc+`"`)
		if len(quoted) >= 24 {
			break
		}
	}
	op := " OR "
	if requireAll {
		op = " AND "
	}
	return strings.Join(quoted, op)
}

func isCJK(r rune) bool {
	return unicode.In(r, unicode.Han, unicode.Hiragana, unicode.Katakana, unicode.Hangul)
}

func isASCIIToken(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-'
}
