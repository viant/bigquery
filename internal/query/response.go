package query

import (
	"github.com/viant/bigquery/internal"
	"google.golang.org/api/bigquery/v2"
)

//Response represents query call response
type Response struct {
	bigquery.QueryResponse
	session *internal.Session
}
