package ingestion

import (
	"fmt"
	"github.com/viant/parsly"
)

func Parse(SQL string) (*Ingestion, error) {
	cursor := parsly.NewCursor("", []byte(SQL), 0)
	match := cursor.MatchAfterOptional(whitespaceMatcher, loadKeywordMatcher)
	fmt.Printf("matched: %v %T\n", match.Code, match.Matcher)
	if match.Code != loadKeyword {
		return nil, cursor.NewError(loadKeywordMatcher)
	}
	match = cursor.MatchAfterOptional(whitespaceMatcher, readOptionsMatcher)
	fmt.Printf("matched: %v %T\n", match.Code, match.Matcher)
	if match.Code != readerOptions {
		return nil, cursor.NewError(loadKeywordMatcher)
	}

	result := &Ingestion{}
	matched := match.Text(cursor)
	fmt.Printf("matched: %v n", matched)

	return result, nil

}
