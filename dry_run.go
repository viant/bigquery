package bigquery

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	api "google.golang.org/api/bigquery/v2"
)

// DryRun validates a query and returns BigQuery's processing statistics without
// running it or fetching query results. The connection owns credentials, billing
// project, default dataset and location. Missing statistics remain unknown.
func DryRun(ctx context.Context, db *sql.DB, query string, args ...driver.NamedValue) (*api.Job, error) {
	if db == nil {
		return nil, fmt.Errorf("bigquery: database is required")
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var job *api.Job
	err = conn.Raw(func(raw any) error {
		capability, ok := raw.(interface {
			DryRun(context.Context, string, []driver.NamedValue) (*api.Job, error)
		})
		if !ok {
			return fmt.Errorf("bigquery: connection does not support dry run")
		}
		job, err = capability.DryRun(ctx, query, args)
		return err
	})
	return job, err
}

// DryRun is available to database/sql callers through Conn.Raw.
func (c *connection) DryRun(ctx context.Context, query string, args []driver.NamedValue) (*api.Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.isIngestion(query) {
		return nil, fmt.Errorf("bigquery: dry run requires a query")
	}
	prepared, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer prepared.Close()
	statement, ok := prepared.(*Statement)
	if !ok {
		return nil, fmt.Errorf("bigquery: dry run requires a query statement")
	}
	params, err := NamedValues(args).QueryParameter()
	if err != nil {
		return nil, err
	}
	if err = statement.bindParameters(params); err != nil {
		return nil, err
	}
	configuration := statement.job.Configuration.Query
	if configuration.ParameterMode == "" && len(configuration.QueryParameters) > 0 {
		named := configuration.QueryParameters[0].Name != ""
		for _, p := range configuration.QueryParameters {
			if (p.Name != "") != named {
				return nil, fmt.Errorf("bigquery: cannot mix named and positional query parameters")
			}
		}
		if named {
			configuration.ParameterMode = "NAMED"
		} else {
			configuration.ParameterMode = "POSITIONAL"
		}
	}
	statement.job.Configuration.DryRun = true
	return statement.submitJob(ctx)
}
