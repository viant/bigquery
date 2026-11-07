package bigquery

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/bigquery/reader"
	api "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConnectorJobProjectIsOptIn(t *testing.T) {
	for _, tc := range []struct{ name, job, want string }{{"unchanged default", "", "data-project"}, {"explicit job project", "job-project", "job-project"}} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseDSN("bigquery://data-project/records?emulator=true&endpoint=http%3A%2F%2F127.0.0.1%3A19050%2F&jobProject=" + tc.job)
			require.NoError(t, err)
			driverConnector, err := NewConnector(cfg)
			require.NoError(t, err)
			raw, err := driverConnector.Connect(context.Background())
			require.NoError(t, err)
			defer raw.Close()
			conn := raw.(*connection)
			require.Equal(t, tc.want, conn.projectID)
			require.Equal(t, "data-project", conn.cfg.ProjectID)
			job, err := conn.jobConfiguration("SELECT 1")
			require.NoError(t, err)
			require.Equal(t, "data-project", job.Configuration.Query.DefaultDataset.ProjectId)
		})
	}
}

func TestQueryCachePreservedInBothRequestModes(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		want      *bool
	}{{"server default", "SELECT 1", nil}, {"enabled", `SELECT /*+ {"useQueryCache":true} +*/ 1`, boolPointer(true)}, {"disabled", `SELECT /*+ {"useQueryCache":false} +*/ 1`, boolPointer(false)}} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &connection{cfg: &Config{ProjectID: "data-project", JobProject: "job-project"}, projectID: "job-project"}
			job, err := conn.jobConfiguration(tc.sql)
			require.NoError(t, err)
			require.Equal(t, tc.want, job.Configuration.Query.UseQueryCache)
			request := (&Statement{queryMode: "sync", job: job}).synchronousRequest()
			require.NotNil(t, request)
			require.Equal(t, tc.want, request.UseQueryCache)
		})
	}
}
func boolPointer(v bool) *bool { return &v }

func TestJobProjectDoesNotRedirectStreamingDestination(t *testing.T) {
	paths := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths <- r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	service, err := api.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
	require.NoError(t, err)
	conn := &connection{cfg: &Config{ProjectID: "data-project", JobProject: "job-project", DatasetID: "records"}, projectID: "job-project", service: service}
	require.NoError(t, reader.Register("job-project-fixture", strings.NewReader(`{"id":1}`)))
	defer reader.Unregister("job-project-fixture")
	statement, err := conn.PrepareContext(context.Background(), "STREAM 'Reader::json:job-project-fixture' DATA INTO TABLE events")
	require.NoError(t, err)
	defer statement.Close()
	_, err = statement.Exec(nil)
	require.NoError(t, err)
	require.Equal(t, "/projects/data-project/datasets/records/tables/events/insertAll", <-paths)
}
