package bigquery

import (
	"database/sql/driver"
	"fmt"
	"github.com/viant/bigquery/internal/param"
	"google.golang.org/api/bigquery/v2"
)

// Values represents value slice
type Values []driver.Value

// QueryParameter converts value to query parameters
func (v Values) QueryParameter() ([]*bigquery.QueryParameter, error) {
	var result = make([]*bigquery.QueryParameter, len(v))
	for i := range v {
		p := param.New("", v[i])
		queryParam, err := p.QueryParameter()
		if err != nil {
			return nil, fmt.Errorf("query parameter at position %d: %w", i+1, err)
		}
		result[i] = queryParam
	}
	return result, nil
}

// NamedValues represents name values slice
type NamedValues []driver.NamedValue

// QueryParameter converts value to query parameters
func (v NamedValues) QueryParameter() ([]*bigquery.QueryParameter, error) {
	var result = make([]*bigquery.QueryParameter, len(v))
	for i, item := range v {
		p := param.New(item.Name, v[i].Value)
		queryParam, err := p.QueryParameter()
		if err != nil {
			if item.Name != "" {
				return nil, fmt.Errorf("query parameter %q: %w", item.Name, err)
			}
			position := item.Ordinal
			if position <= 0 {
				position = i + 1
			}
			return nil, fmt.Errorf("query parameter at position %d: %w", position, err)
		}
		result[i] = queryParam
	}
	return result, nil
}
