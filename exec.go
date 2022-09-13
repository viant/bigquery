package bigquery

import (
	"context"
	"database/sql/driver"
	"github.com/viant/bigquery/internal/ingestion"
)

type ingestionStatement struct {
	service *ingestion.Service
	ctx     context.Context
	SQL     string
}

// Close dummy function because interface requires it
func (s *ingestionStatement) Close() error {
	return nil
}

// NumInput dummy function because interface requires it
func (s *ingestionStatement) NumInput() int {
	return 0
}

// Exec executes a query that doesn't return rows, such as an LOAD
func (s *ingestionStatement) Exec(args []driver.Value) (driver.Result, error) {
	affected, err := s.service.Ingest(s.ctx, s.SQL)
	res := result{}
	res.totalRows = affected
	return &res, err
}

// Query dummy function because interface requires it
func (s *ingestionStatement) Query(args []driver.Value) (driver.Rows, error) {
	return &Rows{}, nil
}
