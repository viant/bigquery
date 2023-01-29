package bigquery

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/viant/bigquery/internal/exec"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

//Statement abstraction implements database/sql driver.Statement interface
type Statement struct {
	projectID string
	location  string
	service   *bigquery.Service
	job       *bigquery.Job
	numInput  int
}

func (s *Statement) submitJob(ctx context.Context) (*bigquery.Job, error) {
	queryJob := s.job
	if queryJob.JobReference == nil {
		queryJob.JobReference = &bigquery.JobReference{}
	}
	queryJob.JobReference.ProjectId = s.projectID
	queryJob.JobReference.Location = s.location
	var job *bigquery.Job
	var err error
	err = exec.RunWithRetries(func() error {
		jobCall := s.service.Jobs.Insert(s.projectID, queryJob)
		job, err = jobCall.Context(ctx).Do()
		return err
	}, 3)
	return job, err
}

//Exec executes statements
func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	params, err := Values(args).QueryParameter()
	if err != nil {
		return nil, fmt.Errorf("failed to convert args to query parameters: %w", err)
	}
	return s.exec(context.Background(), params)
}

//ExecContext executes statements
func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	params, err := NamedValues(args).QueryParameter()
	if err != nil {
		return nil, fmt.Errorf("failed to convert args to query parameters: %w", err)
	}
	return s.exec(ctx, params)
}

func (s *Statement) exec(ctx context.Context, params []*bigquery.QueryParameter) (driver.Result, error) {
	s.job.Configuration.Query.QueryParameters = params
	job, err := s.submitJob(ctx)
	if err != nil {
		return nil, err
	}
	completed, err := exec.WaitForJobCompletion(ctx, s.service, s.projectID, s.location, job.JobReference.JobId)
	if err != nil {
		return nil, fmt.Errorf("failed to run job: %v.%v, %w", job.JobReference.ProjectId, job.JobReference.JobId, err)
	}
	res := result{}
	if stats := completed.Statistics; stats != nil {
		if queryStats := stats.Query; queryStats != nil {
			res.totalRows = queryStats.NumDmlAffectedRows
		}
	}
	return &res, nil
}

//Query runs query
func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	params, err := Values(args).QueryParameter()
	if err != nil {
		return nil, fmt.Errorf("failed to convert args to query parameters: %w", err)
	}
	return s.query(context.Background(), params)
}

//QueryContext runs query
func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	params, err := NamedValues(args).QueryParameter()
	if err != nil {
		return nil, fmt.Errorf("failed to convert args to query parameters: %w", err)
	}
	return s.query(ctx, params)
}

func (s *Statement) query(ctx context.Context, params []*bigquery.QueryParameter) (driver.Rows, error) {
	s.job.Configuration.Query.QueryParameters = params
	job, err := s.submitJob(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w, SQL: %v", err, s.job.Configuration.Query.Query)
	}
	if job.Status.State != exec.StatusDone {
		if _, err = exec.WaitForJobCompletion(ctx, s.service, s.projectID, s.location, job.JobReference.JobId); err != nil {
			return nil, fmt.Errorf("%w, SQL: %v", err, s.job.Configuration.Query.Query)
		}
	}
	return newRows(s.service, s.projectID, s.location, job)
}

//Close closes statement
func (s *Statement) Close() error {
	s.service = nil
	return nil
}

//NumInput returns numinput
func (s *Statement) NumInput() int {
	return s.numInput
}

//CheckNamedValue checks name values
func (s *Statement) CheckNamedValue(n *driver.NamedValue) error {
	return nil
}

func (s *Statement) checkQueryParameters() {
	//this is very basic parameter detection, need to be improved
	query := strings.ToLower(s.job.Configuration.Query.Query)
	count := checkQueryParameters(query)
	s.numInput = count
}

func checkQueryParameters(query string) int {
	count := 0
	inQuote := false
	for i, c := range query {
		switch c {
		case '\'':
			if i > 1 && inQuote && query[i-1] == '\\' {
				continue
			}
			inQuote = !inQuote
		case '?', '@':
			if !inQuote {
				count++
			}
		}
	}
	return count
}
