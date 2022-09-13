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
	readerKeyword
	dataIntoSequence
	destination
	dataFormat
)

var whitespaceMatcher = parsly.NewToken(whitespace, "WHITESPACE", matcher.NewWhiteSpace())
var loadKeywordMatcher = parsly.NewToken(loadKeyword, "LOAD", matcher.NewFragment("LOAD", &option.Case{Sensitive: false}))

var readOptionsMatcher = parsly.NewToken(readerOptions, "'Reader:<Format>:<ReaderID>'", matcher.NewByteQuote('\'', '\\'))
var readerKeywordMatcher = parsly.NewToken(readerKeyword, "Reader", matcher.NewFragment("READER", &option.Case{Sensitive: false}))

var dataFormatMatcher = parsly.NewToken(dataFormat, "<CSV|JSON>", matcher.NewSet([]string{"CSV", "JSON"}, &option.Case{Sensitive: false}))

var dataIntoSequenceMatcher = parsly.NewToken(dataIntoSequence, "DATA INTO TABLE", matcher.NewSpacedFragment("DATA INTO TABLE", &option.Case{Sensitive: false}))
var destinationMatcher = parsly.NewToken(destination, "project.set.table", smatcher.NewSelector())
