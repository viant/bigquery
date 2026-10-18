package query

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/bigquery/internal"
	"google.golang.org/api/bigquery/v2"
)

// Response represents query call response
type Response struct {
	bigquery.QueryResponse
	session *internal.Session
}

// Decode initializes the shared row decoder from a query API response.
func Decode(data []byte, session *internal.Session) (*Response, error) {
	session.Data = data
	session.Rows = nil
	result := &Response{session: session}
	if err := gojay.UnmarshalJSONObject(data, result); err != nil {
		return nil, fmt.Errorf("decode query response: %w", err)
	}
	if err := finalizeRows(session); err != nil {
		return nil, err
	}
	return result, nil
}

// A rows array may be the final JSON field; there is then no subsequent
// field callback to close the last row region in the streaming decoder.
func finalizeRows(session *internal.Session) error {
	if len(session.Rows) == 0 {
		return nil
	}
	last := &session.Rows[len(session.Rows)-1]
	if last.End != 0 {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(session.Data[last.Begin:]))
	var row json.RawMessage
	if err := decoder.Decode(&row); err != nil {
		return fmt.Errorf("decode final row: %w", err)
	}
	last.End = last.Begin + int(decoder.InputOffset())
	return nil
}
