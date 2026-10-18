package bigquery

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bigquery/internal/exec"
	"github.com/viant/bigquery/internal/query"
	"google.golang.org/api/bigquery/v2"
)

// Use the query API only when it can represent every configured job option.
// Unsupported job options retain the existing jobs.insert execution path.
func (s *Statement) synchronousRequest() *bigquery.QueryRequest {
	if s.queryMode != "sync" || s.job.Configuration.Reservation != "" {
		return nil
	}
	config := s.job.Configuration.Query
	if config.Priority != "" && config.Priority != "INTERACTIVE" {
		return nil
	}
	data, err := json.Marshal(config)
	if err != nil {
		return nil
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil {
		return nil
	}
	delete(fields, "priority") // The query API is interactive.
	known := map[string]bool{}
	typ := reflect.TypeOf(bigquery.QueryRequest{})
	for i := 0; i < typ.NumField(); i++ {
		known[strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]] = true
	}
	for name := range fields {
		if !known[name] {
			return nil
		}
	}
	data, err = json.Marshal(fields)
	if err != nil {
		return nil
	}
	request := &bigquery.QueryRequest{}
	if json.Unmarshal(data, request) != nil {
		return nil
	}
	request.Location = s.location
	return request
}

func (s *Statement) querySynchronous(ctx context.Context, request *bigquery.QueryRequest) (driver.Rows, error) {
	// Do not blindly retry submission: a query may contain mutations.
	result, err := s.service.Jobs.Query(s.projectID, request).Context(ctx).Do()
	if err != nil {
		return nil, err
	}
	job := &bigquery.Job{JobReference: result.JobReference}
	if job.JobReference == nil {
		return nil, fmt.Errorf("BigQuery query response is missing its job reference")
	}
	// QueryResponse.Errors may contain nonfatal warnings. Inspect the job status
	// before treating them as an execution failure.
	if !result.JobComplete || len(result.Errors) > 0 {
		if _, err := exec.WaitForJobCompletion(ctx, s.service, s.projectID, s.location, job.JobReference.JobId); err != nil {
			return nil, err
		}
		return newRows(s.service, s.projectID, s.location, job)
	}
	data, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	rows := &Rows{service: s.service, projectID: s.projectID, location: s.location, job: job}
	decoded, err := query.Decode(data, &rows.session)
	if err != nil {
		return nil, err
	}
	rows.pageToken = decoded.PageToken
	return rows, nil
}
