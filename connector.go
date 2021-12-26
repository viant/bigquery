package bigquery

import (
	"context"
	"database/sql/driver"
	"google.golang.org/api/bigquery/v2"
)

type connector struct {
	cfg *Config
}

//Connect connects to database
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	service, err := bigquery.NewService(ctx, c.cfg.options()...)
	if err != nil {
		return nil, err
	}
	return &connection{
		ctx:       ctx,
		service:   service,
		cfg:       c.cfg,
		projectID: c.cfg.ProjectID,
	}, nil
}

//Driver returns a driver
func (c *connector) Driver() driver.Driver {
	return &Driver{}
}
