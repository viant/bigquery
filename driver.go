package bigquery

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"google.golang.org/api/option"
)

// Driver is exported to make the driver directly accessible.
// In general the driver is used via the database/sql package.
type Driver struct{}

// Open new Connection.
// See https://github.com/viant/bigquery#dsn-data-source-name for how
// the DSN string is formatted
func (d Driver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c := &connector{
		cfg: cfg,
	}
	return c.Connect(context.Background())
}

//SetGCPClientOptions sets global gcp options
func (d *Driver) SetGCPClientOptions(options ...option.ClientOption) {
	SetOptions(options...)
}

func init() {
	sql.Register("bigquery", &Driver{})
}

// NewConnector returns new driver.Connector.
func NewConnector(cfg *Config) (driver.Connector, error) {
	return &connector{cfg: cfg}, nil
}

// OpenConnector implements driver.DriverContext.
func (d Driver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &connector{
		cfg: cfg,
	}, nil
}
