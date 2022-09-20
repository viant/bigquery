package ingestion

import (
	"fmt"
	"github.com/viant/parsly"
	"strings"
)

// parse creates ingestion from parsed SQL
func parse(SQL string) (*ingestion, error) {
	result := &ingestion{}
	cursor := parsly.NewCursor("", []byte(SQL), 0)

	match := cursor.MatchOne(ingestionKindMatcher)
	if match.Code != ingestionKindKeyword {
		return nil, fmt.Errorf("%w, current token:%s", cursor.NewError(ingestionKindMatcher), SQL)
	}
	result.Kind = kind(match.Text(cursor))

	match = cursor.MatchOne(whitespaceMatcher)
	if match.Code != whitespace {
		return nil, fmt.Errorf("%w, current token:%s", cursor.NewError(whitespaceMatcher), SQL)
	}

	match = cursor.MatchAfterOptional(whitespaceMatcher, readOptionsMatcher)
	if match.Code != readerOptions {
		return nil, fmt.Errorf("%w, current token:%s", cursor.NewError(readOptionsMatcher), SQL)
	}

	encodedReaderOption := match.Text(cursor)
	encodedReaderOption = encodedReaderOption[1 : len(encodedReaderOption)-1]

	var err error
	switch result.Kind {
	case KindStream:
		err = decodeReaderOptionsForStream(encodedReaderOption, result)
	default:
		err = decodeReaderOptionsForLoad(encodedReaderOption, result)
	}

	if err != nil {
		return nil, err
	}

	match = cursor.MatchOne(whitespaceMatcher)
	if match.Code != whitespace {
		return nil, fmt.Errorf("%w, current token:%s", cursor.NewError(whitespaceMatcher), SQL)
	}

	match = cursor.MatchAfterOptional(whitespaceMatcher, dataIntoSequenceMatcher)
	if match.Code != dataIntoSequence {
		return nil, fmt.Errorf("%w, current token:%s", cursor.NewError(dataIntoSequenceMatcher), SQL)
	}

	match = cursor.MatchOne(whitespaceMatcher)
	if match.Code != whitespace {
		return nil, fmt.Errorf("%w, current token:%s", cursor.NewError(whitespaceMatcher), SQL)
	}

	match = cursor.MatchAfterOptional(whitespaceMatcher, destinationMatcher)
	if match.Code != aDestination {
		return nil, fmt.Errorf("%w, current token:%s", cursor.NewError(destinationMatcher), SQL)
	}

	encodedDestination := match.Text(cursor)
	err = decodeDestination(encodedDestination, result)
	if err != nil {
		return nil, err
	}

	match = cursor.MatchOne(whitespaceMatcher)
	switch match.Code {
	case whitespace:
		if cursor.HasMore() {
			return nil, fmt.Errorf("unexpected sequence: %s", cursor.Input[cursor.Pos:])
		}
	case parsly.EOF:
	default:
		return nil, fmt.Errorf("unexpected sequence: %s", cursor.Input[cursor.Pos:])
	}

	return result, nil
}

func decodeReaderOptionsForLoad(text string, ingestion *ingestion) error {

	opts := strings.SplitN(text, ":", 3)
	if len(opts) != 3 {
		return fmt.Errorf("failed to split reader options:%s, supported:[%s]", text, readOptionsMatcher.Name)
	}

	cursor := parsly.NewCursor("", []byte(opts[0]), 0)
	match := cursor.MatchOne(readerKeywordMatcher)
	if match.Code != readerKeyword || cursor.HasMore() {
		return fmt.Errorf("%w, current token:%s", cursor.NewError(readerKeywordMatcher), opts[0])
	}

	cursor = parsly.NewCursor("", []byte(opts[1]), 0)
	match = cursor.MatchOne(dataFormatMatcher)
	if match.Code != dataFormat || cursor.HasMore() {
		return fmt.Errorf("%w, current token:%s", cursor.NewError(dataFormatMatcher), opts[1])
	}
	ingestion.Format = opts[1]
	ingestion.ReaderID = opts[2]

	return nil
}

func decodeReaderOptionsForStream(text string, ingestion *ingestion) error {

	opts := strings.SplitN(text, ":", 4)
	if len(opts) != 4 {
		return fmt.Errorf("failed to split reader options:%s, supported:[%s]", text, readOptionsMatcher.Name)
	}

	cursor := parsly.NewCursor("", []byte(opts[0]), 0)
	match := cursor.MatchOne(readerKeywordMatcher)
	if match.Code != readerKeyword || cursor.HasMore() {
		return fmt.Errorf("%w, current token:%s", cursor.NewError(readerKeywordMatcher), opts[0])
	}

	ingestion.InsertIDField = opts[1]

	cursor = parsly.NewCursor("", []byte(opts[2]), 0)
	match = cursor.MatchOne(dataFormatMatcher)
	if match.Code != dataFormat || cursor.HasMore() {
		return fmt.Errorf("%w, current token:%s", cursor.NewError(dataFormatMatcher), opts[1])
	}
	ingestion.Format = opts[2]
	ingestion.ReaderID = opts[3]

	return nil
}

// decodeDestination updates ingestion with decoded destination values
func decodeDestination(text string, ingestion *ingestion) error {

	opts := strings.SplitN(text, ".", 3)
	ingestion.Destination = &destination{}

	switch len(opts) {
	case 3:
		ingestion.Destination.TableID = opts[2]
		ingestion.Destination.DatasetID = opts[1]
		ingestion.Destination.ProjectID = opts[0]

	case 2:
		ingestion.Destination.TableID = opts[1]
		ingestion.Destination.DatasetID = opts[0]
	case 1:
		ingestion.Destination.TableID = opts[0]
	case 0:
		return fmt.Errorf("failed to split destination string:%s, supported:[%s]", text, destinationMatcher.Name)
	}

	dest := ingestion.Destination
	if !isValidSelector(dest.TableID) {
		return fmt.Errorf("invalid table: %v", dest.TableID)
	}
	if dest.DatasetID != "" {
		if !isValidSelector(dest.DatasetID) {
			return fmt.Errorf("invalid dataset : %v", dest.DatasetID)
		}

	}
	if dest.ProjectID != "" {
		if !isValidSelector(dest.ProjectID) {
			return fmt.Errorf("invalid project : %v", dest.ProjectID)
		}
	}
	return nil
}

func isValidSelector(id string) bool {
	cursor := parsly.NewCursor("", []byte(id), 0)
	match := cursor.MatchOne(destinationMatcher)
	return match.Code == aDestination
}
