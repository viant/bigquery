package bigquery

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
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
	return c.PrepareContext(context.TODO(), query)
}

// PrepareContext returns a prepared statement, bound to this connection.
func (c *connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	configQuery, err := c.configureQuery(query)
	if err != nil {
		return nil, err
	}
	stmt := &Statement{configQuery: configQuery, service: c.service, projectID: c.projectID, location: c.cfg.Location}

	stmt.checkQueryParameters()
	return stmt, nil
}

func (c *connection) configureQuery(query string) (*bigquery.JobConfigurationQuery, error) {
	useLegacy := false
	configQuery := &bigquery.JobConfigurationQuery{UseLegacySql: &useLegacy}

	if index := strings.Index(query, "/*+"); index != -1 {
		if end := strings.Index(query, "+*/"); end != -1 {
			hint := strings.TrimSpace(query[index+3 : end])
			if err := json.Unmarshal([]byte(hint), configQuery); err != nil {
				return nil, fmt.Errorf("invalid hint %v, %w", hint, err)
			}
		}
	}
	configQuery.Query = query
	if c.cfg.DatasetID != "" {
		configQuery.DefaultDataset = &bigquery.DatasetReference{
			ProjectId: c.projectID,
			DatasetId: c.cfg.DatasetID,
		}
	}
	return configQuery, nil
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
