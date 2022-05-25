package bigquery

import (
	"context"
	"database/sql/driver"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

type connector struct {
	cfg     *Config
	options []option.ClientOption
}

var globalOptions []option.ClientOption

//SetOptions sets global client options
func SetOptions(opts ...option.ClientOption) {
	globalOptions = opts
}

//Connect connects to database
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	options := c.cfg.options()
	if len(c.options) > 0 {
		options = append(options, c.options...)
	} else if len(globalOptions) > 0 {
		options = append(options, globalOptions...)
	}
	service, err := bigquery.NewService(ctx, options...)
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
