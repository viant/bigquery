package bigquery

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	api "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

func TestSynchronousQueryPagesAndPendingJob(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "pending"}[pending], func(t *testing.T) {
			var submissions, polls, resultReads int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == "POST" && r.URL.Path == "/projects/p/queries":
					submissions++
					var request api.QueryRequest
					require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
					require.Equal(t, "SELECT @value", request.Query)
					require.Equal(t, "42", request.QueryParameters[0].ParameterValue.Value)
					if pending {
						io.WriteString(w, `{"jobComplete":false,"jobReference":{"projectId":"p","jobId":"j"}}`)
						return
					}
					io.WriteString(w, `{"jobComplete":true,"jobReference":{"projectId":"p","jobId":"j"},"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"totalRows":"2","pageToken":"next","rows":[{"f":[{"v":"42"}]}]}`)
				case r.URL.Path == "/projects/p/jobs/j":
					polls++
					io.WriteString(w, `{"jobReference":{"projectId":"p","jobId":"j"},"status":{"state":"DONE"}}`)
				case r.URL.Path == "/projects/p/queries/j":
					resultReads++
					if pending && r.URL.Query().Get("pageToken") == "" {
						io.WriteString(w, `{"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"totalRows":"2","pageToken":"next","rows":[{"f":[{"v":"42"}]}]}`)
					} else {
						require.Equal(t, "next", r.URL.Query().Get("pageToken"))
						io.WriteString(w, `{"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"totalRows":"2","rows":[{"f":[{"v":"43"}]}]}`)
					}
				default:
					t.Errorf("unexpected API call: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(500)
				}
			}))
			defer server.Close()
			service, err := api.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
			require.NoError(t, err)
			conn := &connection{cfg: &Config{QueryMode: "sync"}, projectID: "p", service: service}
			stmt, err := conn.Prepare("SELECT @value")
			require.NoError(t, err)
			rows, err := stmt.(*Statement).QueryContext(context.Background(), []driver.NamedValue{{Name: "value", Value: int64(42)}})
			require.NoError(t, err)
			defer rows.Close()
			values := make([]driver.Value, 1)
			require.NoError(t, rows.Next(values))
			require.EqualValues(t, 42, values[0])
			require.NoError(t, rows.Next(values))
			require.EqualValues(t, 43, values[0])
			require.Equal(t, io.EOF, rows.Next(values))
			require.Equal(t, 1, submissions)
			if pending {
				require.Equal(t, 1, polls)
				require.Equal(t, 2, resultReads)
			} else {
				require.Zero(t, polls)
				require.Equal(t, 1, resultReads)
			}
		})
	}
}

func TestSynchronousQueryPreservesUnsupportedJobOptions(t *testing.T) {
	conn := &connection{cfg: &Config{QueryMode: "sync"}, projectID: "p"}
	prepared, err := conn.Prepare("SELECT 1")
	require.NoError(t, err)
	stmt := prepared.(*Statement)
	require.NotNil(t, stmt.synchronousRequest())
	stmt.job.Configuration.Query.DestinationTable = &api.TableReference{TableId: "target"}
	require.Nil(t, stmt.synchronousRequest())
	stmt.job.Configuration.Query.DestinationTable = nil
	stmt.job.Configuration.Query.Priority = "BATCH"
	require.Nil(t, stmt.synchronousRequest())
	_, err = ParseDSN("bigquery://p/d?queryMode=unknown")
	require.Error(t, err)
}

func TestSynchronousQueryErrors(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"http", `{"error":{"code":400,"message":"invalid query"}}`, 400},
		{"query", `{"jobComplete":true,"errors":[{"message":"execution failed"}]}`, 200},
		{"missing job", `{"jobComplete":true}`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer server.Close()
			service, err := api.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
			require.NoError(t, err)
			stmt := &Statement{service: service, projectID: "p"}
			_, err = stmt.querySynchronous(context.Background(), &api.QueryRequest{Query: "SELECT 1"})
			require.Error(t, err)
			require.Equal(t, 1, calls)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = stmt.querySynchronous(ctx, &api.QueryRequest{Query: "SELECT 1"})
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, 1, calls)
		})
	}
}

func TestSynchronousQueryWarningsCheckJobStatus(t *testing.T) {
	for _, fatal := range []bool{false, true} {
		t.Run(map[bool]string{false: "warning", true: "fatal"}[fatal], func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == "POST":
					io.WriteString(w, `{"jobComplete":true,"jobReference":{"projectId":"p","jobId":"j"},"errors":[{"message":"diagnostic"}]}`)
				case r.URL.Path == "/projects/p/jobs/j":
					if fatal {
						io.WriteString(w, `{"status":{"state":"DONE","errorResult":{"message":"query failed"}}}`)
					} else {
						io.WriteString(w, `{"status":{"state":"DONE"}}`)
					}
				default:
					io.WriteString(w, `{"jobComplete":true,"schema":{"fields":[{"name":"n","type":"INTEGER","mode":"NULLABLE"}]},"totalRows":"0"}`)
				}
			}))
			defer server.Close()
			service, err := api.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
			require.NoError(t, err)
			stmt := &Statement{service: service, projectID: "p"}
			rows, err := stmt.querySynchronous(context.Background(), &api.QueryRequest{Query: "SELECT 1"})
			if fatal {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				defer rows.Close()
				require.Equal(t, io.EOF, rows.Next(make([]driver.Value, 1)))
			}
		})
	}
}
