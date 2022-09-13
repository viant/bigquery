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
	//	hintOptions
	dataIntoSequence
	aDestination
	dataFormat
)

var whitespaceMatcher = parsly.NewToken(whitespace, "WHITESPACE", matcher.NewWhiteSpace())
var loadKeywordMatcher = parsly.NewToken(loadKeyword, "LOAD", matcher.NewFragment("LOAD", &option.Case{Sensitive: false}))

var readOptionsMatcher = parsly.NewToken(readerOptions, "'Reader:<Format>:<ReaderID>'", matcher.NewByteQuote('\'', '\\'))
var readerKeywordMatcher = parsly.NewToken(readerKeyword, "Reader", matcher.NewFragment("READER", &option.Case{Sensitive: false}))

var dataFormatMatcher = parsly.NewToken(dataFormat, "<CSV|JSON|PARQUET>", matcher.NewSet([]string{"CSV", "JSON", "PARQUET"}, &option.Case{Sensitive: false}))

// TODO problem with parsing quota mark inside block e.g.: /*"*/
//var hintMatcher = parsly.NewToken(hintOptions, "/*+ HINT +*/", matcher.NewSeqBlock("/*+", "+*/"))

var dataIntoSequenceMatcher = parsly.NewToken(dataIntoSequence, "DATA INTO TABLE", matcher.NewSpacedFragment("DATA INTO TABLE", &option.Case{Sensitive: false}))
var destinationMatcher = parsly.NewToken(aDestination, "project.set.table", smatcher.NewSelector())
