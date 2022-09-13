package bigquery

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/viant/bigquery/internal/hint"
	"github.com/viant/bigquery/internal/ingestion"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

type connection struct {
	cfg       *Config
	projectID string
	ctx       context.Context
	service   *bigquery.Service
}

// Prepare returns a prepared statement, bound to this connection.
func (c *connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *connection) isIngestion(SQL string) bool {
	normalizedSQL := strings.ToUpper(strings.TrimSpace(SQL))
	return strings.HasPrefix(normalizedSQL, string(ingestion.KindLoad)) || strings.HasPrefix(normalizedSQL, string(ingestion.KindStream))
}

// PrepareContext returns a prepared statement, bound to this connection.
func (c *connection) PrepareContext(ctx context.Context, SQL string) (driver.Stmt, error) {

	if c.isIngestion(SQL) {
		return &ingestionStatement{
			service: ingestion.NewService(c.service, c.projectID, c.cfg.DatasetID, c.cfg.Location),
			ctx:     ctx,
			SQL:     SQL,
		}, nil
	}

	jobConfiguration, err := c.jobConfiguration(SQL)
	if err != nil {
		return nil, err
	}

	stmt := &Statement{job: jobConfiguration, service: c.service, projectID: c.projectID, location: c.cfg.Location}
	stmt.checkQueryParameters()
	return stmt, nil
}

func (c *connection) jobConfiguration(query string) (*bigquery.Job, error) {
	job := &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{},
	}
	useLegacy := false
	configQuery := &bigquery.JobConfigurationQuery{UseLegacySql: &useLegacy}

	if aHint := hint.Extract(query); aHint != "" {
		userHint := &queryHint{
			JobConfigurationQuery: bigquery.JobConfigurationQuery{
				UseLegacySql: &useLegacy,
			},
		}
		if err := json.Unmarshal([]byte(aHint), &userHint); err != nil {
			return nil, fmt.Errorf("invalid aHint %v, %w", aHint, err)
		}
		if userHint.ExpandDSN {
			if count := strings.Count(query, dsnProjectID); count > 0 {
				query = strings.Replace(query, dsnProjectID, c.cfg.ProjectID, count)
			}
			if count := strings.Count(query, dsnDatasetID); count > 0 {
				query = strings.Replace(query, dsnDatasetID, c.cfg.DatasetID, count)
			}
			if count := strings.Count(query, dsnLocation); count > 0 {
				query = strings.Replace(query, dsnLocation, c.cfg.Location, count)
			}
		}
		configQuery = &userHint.JobConfigurationQuery
	}

	configQuery.Query = query
	if c.cfg.DatasetID != "" {
		configQuery.DefaultDataset = &bigquery.DatasetReference{
			ProjectId: c.projectID,
			DatasetId: c.cfg.DatasetID,
		}
	}
	job.Configuration.Query = configQuery
	return job, nil
}

//Ping pings server
func (c *connection) Ping(ctx context.Context) error {
	return nil
}

// Begin starts and returns a new transaction.
func (c *connection) Begin() (driver.Tx, error) {
	return &tx{c}, nil
}

// BeginTx starts and returns a new transaction.
func (c *connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return &tx{c}, nil
}

// Close closes connection
func (c *connection) Close() error {
	c.service = nil
	return nil
}

//ResetSession resets session
func (c *connection) ResetSession(ctx context.Context) error {
	return nil
}

//IsValid check is connection is valid
func (c *connection) IsValid() bool {
	return true
}
