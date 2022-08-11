package bigquery

import (
	"database/sql/driver"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/bigquery/internal"
	"github.com/viant/bigquery/internal/query"
	"google.golang.org/api/bigquery/v2"
	"io"
	"reflect"
)

// Rows abstraction implements database/sql driver.Rows interface
type Rows struct {
	session       internal.Session
	projectID     string
	location      string
	service       *bigquery.Service
	job           *bigquery.Job
	pageToken     string
	processedRows uint64
	pageIndex     int
}

// Columns returns query columns
func (r *Rows) Columns() []string {
	return r.session.Columns
}

// Close closes rows
func (r *Rows) Close() error {
	r.service = nil
	return nil
}

// Next moves to next row
func (r *Rows) Next(dest []driver.Value) error {
	if !r.hasNext() {
		return io.EOF
	}

	if r.pageIndex >= len(r.session.Rows) {
		if err := r.fetchPage(); err != nil {
			return err
		}
	}

	region := r.session.Rows[r.pageIndex]
	data := r.session.Data[region.Begin:region.End]
	err := gojay.UnmarshalJSONArray(data, r.session.Decoder)
	if err != nil {
		return fmt.Errorf("failed to unmarshal Array: %w, %s", err, data)
	}
	for i := range r.session.Pointers {
		dest[i] = r.session.XTypes[i].Deref(r.session.Pointers[i])
	}
	r.pageIndex++
	r.processedRows++
	return nil
}

// hasNext returns true if there is next row to fetch.
func (r *Rows) hasNext() bool {
	return r.processedRows < r.session.TotalRows
}

func (r *Rows) init() error {
	response, err := r.queryResult()
	if err != nil {
		return err
	}
	r.pageToken = response.PageToken
	return nil
}

func (r *Rows) fetchPage() error {
	response, err := r.queryResult()
	if err != nil {
		return err
	}
	r.pageToken = response.PageToken
	r.pageIndex = 0
	return nil
}

func (r *Rows) queryResult() (*query.Response, error) {
	call := r.service.Jobs.GetQueryResults(r.projectID, r.job.JobReference.JobId)
	call.Location(r.location)
	queryCall := query.NewResultsCall(call, &r.session)
	call.PageToken(r.pageToken)
	response, err := queryCall.Do()
	return response, err
}

// ColumnTypeScanType returns column scan type
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	return r.session.DestTypes[index]
}

// ColumnTypeDatabaseTypeName returns column database type name
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	return r.session.Schema.Fields[index].Type
}

// ColumnTypeNullable returns if column is nullable
func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	isNullable := r.session.Schema.Fields[index].Mode == "NULLABLE"
	return isNullable, true
}

func newRows(service *bigquery.Service, projectID string, location string, job *bigquery.Job) (*Rows, error) {
	if service == nil {
		return nil, fmt.Errorf("service was nil")
	}
	var result = &Rows{
		service:   service,
		job:       job,
		location:  location,
		projectID: projectID,
	}

	return result, result.init()
}
