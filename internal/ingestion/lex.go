package ingestion

import (
	smatcher "github.com/viant/bigquery/internal/ingestion/matcher"
	"github.com/viant/parsly"
	"github.com/viant/parsly/matcher"
	"github.com/viant/parsly/matcher/option"
)

const (
	whitespace = iota
	loadKeyword
	readerOptions
	dataIntoSequence
	selector
)

var whitespaceMatcher = parsly.NewToken(whitespace, "WHITESPACE", matcher.NewWhiteSpace())
var loadKeywordMatcher = parsly.NewToken(loadKeyword, "LOAD", matcher.NewFragment("LOAD", &option.Case{Sensitive: false}))
var readOptionsMatcher = parsly.NewToken(readerOptions, "READER OPTIONS", matcher.NewByteQuote('\'', '\\'))
var dataIntoSequenceMatcher = parsly.NewToken(loadKeyword, "DATA INTO TABLE", matcher.NewSpacedFragment("DATA INTO TABLE", &option.Case{Sensitive: false}))
var selectorMatcher = parsly.NewToken(selector, "DESTINATION", smatcher.NewSelector())
