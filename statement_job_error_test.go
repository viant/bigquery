package bigquery

import (
	"context"
	"github.com/stretchr/testify/require"
	api "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSubmissionErrorIsNotLostByPolling(t *testing.T) {
	submissions, polls := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "POST" {
			submissions++
			io.WriteString(w, `{"jobReference":{"projectId":"p","jobId":"j"},"status":{"state":"DONE","errorResult":{"reason":"invalidQuery","message":"invalid JSON assignment"}}}`)
		} else {
			polls++
			io.WriteString(w, `{"status":{"state":"DONE"}}`)
		}
	}))
	defer server.Close()
	service, err := api.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
	require.NoError(t, err)
	stmt := &Statement{projectID: "p", service: service, job: &api.Job{Configuration: &api.JobConfiguration{Query: &api.JobConfigurationQuery{Query: "INSERT INTO t VALUES (1)"}}}}
	_, err = stmt.ExecContext(context.Background(), nil)
	require.ErrorContains(t, err, "invalid JSON assignment")
	require.Equal(t, 1, submissions)
	require.Zero(t, polls)
}
