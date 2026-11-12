package bigquery

import (
	"context"
	"fmt"

	api "google.golang.org/api/bigquery/v2"
)

// ListTableMetadata reads one metadata page using the existing authenticated
// connection. It does not submit a query job or change the default dataset.
// The caller owns continuation; an empty page may still have a NextPageToken.
func (c *connection) ListTableMetadata(ctx context.Context, project, dataset string, pageSize int64, pageToken string) (*api.TableList, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if project == "" || dataset == "" {
		return nil, fmt.Errorf("bigquery: table listing requires project and dataset")
	}
	if pageSize <= 0 {
		return nil, fmt.Errorf("bigquery: table listing requires a positive page size")
	}
	return c.service.Tables.List(project, dataset).MaxResults(pageSize).PageToken(pageToken).Context(ctx).Do()
}

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
