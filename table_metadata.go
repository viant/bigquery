package bigquery

import (
	"context"
	"fmt"

	api "google.golang.org/api/bigquery/v2"
)

// TableMetadata exposes table schema on the existing authenticated connection.
// The source project is independent of the connection's query billing project.
func (c *connection) TableMetadata(ctx context.Context, project, dataset, table string) (*api.Table, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if project == "" || dataset == "" || table == "" {
		return nil, fmt.Errorf("bigquery: table metadata requires project, dataset and table")
	}
	return c.service.Tables.Get(project, dataset, table).View("BASIC").Context(ctx).Do()
}
