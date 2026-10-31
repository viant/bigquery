package bigquery

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	api "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

func TestDryRunReturnsStatisticsWithoutPollingOrFetchingRows(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "/projects/billing/jobs", r.URL.Path)
		var job api.Job
		require.NoError(t, json.NewDecoder(r.Body).Decode(&job))
		require.True(t, job.Configuration.DryRun)
		require.Equal(t, "region", job.JobReference.Location)
		require.Equal(t, "data", job.Configuration.Query.DefaultDataset.ProjectId)
		require.Equal(t, "dataset", job.Configuration.Query.DefaultDataset.DatasetId)
		require.Len(t, job.Configuration.Query.QueryParameters, 1)
		require.Equal(t, "POSITIONAL", job.Configuration.Query.ParameterMode)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":{"state":"DONE"},"statistics":{"totalBytesProcessed":"1234","query":{"totalBytesProcessed":"1234","totalBytesProcessedAccuracy":"PRECISE","statementType":"SELECT","referencedTables":[{"projectId":"data","datasetId":"dataset","tableId":"records"}]}}}`))
	}))
	defer server.Close()
	ctx := context.Background()
	service, err := api.NewService(ctx, option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
	require.NoError(t, err)
	c := &connection{projectID: "billing", cfg: &Config{ProjectID: "data", DatasetID: "dataset", Location: "region"}, service: service}
	job, err := c.DryRun(ctx, "SELECT id FROM records WHERE id=?", []driver.NamedValue{{Ordinal: 1, Value: int64(7)}})
	require.NoError(t, err)
	require.EqualValues(t, 1234, job.Statistics.Query.TotalBytesProcessed)
	require.Len(t, job.Statistics.Query.ReferencedTables, 1)
	require.Equal(t, 1, calls)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = c.DryRun(canceled, "SELECT 1", nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}
