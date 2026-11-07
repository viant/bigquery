package bigquery

import (
	"context"
	"github.com/stretchr/testify/require"
	api "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestConnectionTableMetadata(t *testing.T) {
	calls, status := 0, 200
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		require.Equal(t, "GET", r.Method)
		require.Equal(t, "/projects/source/datasets/data/tables/records", r.URL.Path)
		require.Equal(t, "BASIC", r.URL.Query().Get("view"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status != 200 {
			_, _ = w.Write([]byte(`{"error":{"message":"denied"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"},{"name":"items","type":"RECORD","mode":"REPEATED","fields":[{"name":"label","type":"STRING"}]}]}}`))
	}))
	defer server.Close()
	service, err := api.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
	require.NoError(t, err)
	c := &connection{projectID: "billing", cfg: &Config{ProjectID: "default-source", DatasetID: "default-data"}, service: service}
	table, err := c.TableMetadata(context.Background(), "source", "data", "records")
	require.NoError(t, err)
	require.Len(t, table.Schema.Fields[1].Fields, 1)
	require.Equal(t, "billing", c.projectID)
	job, err := c.jobConfiguration("SELECT 1")
	require.NoError(t, err)
	require.Equal(t, "default-source", job.Configuration.Query.DefaultDataset.ProjectId)
	for _, code := range []int{403, 404} {
		status = code
		_, err = c.TableMetadata(context.Background(), "source", "data", "records")
		var apiErr *googleapi.Error
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, code, apiErr.Code)
	}
	before := calls
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.TableMetadata(ctx, "source", "data", "records")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, before, calls)
}

func TestConnectionTableMetadataCancelsRequest(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	service, err := api.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
	require.NoError(t, err)
	c := &connection{service: service}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := c.TableMetadata(ctx, "source", "data", "records"); result <- err }()
	<-started
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
}
