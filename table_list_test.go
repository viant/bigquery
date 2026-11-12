package bigquery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	api "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func TestConnectionListTableMetadata(t *testing.T) {
	calls, status := 0, http.StatusOK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		require.Equal(t, "GET", r.Method)
		require.Equal(t, "/projects/source/datasets/data/tables", r.URL.Path)
		require.Equal(t, "7", r.URL.Query().Get("maxResults"))
		require.Equal(t, "opaque+/=", r.URL.Query().Get("pageToken"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status != http.StatusOK {
			_, _ = w.Write([]byte(`{"error":{"message":"metadata unavailable"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"tables":[],"nextPageToken":"next"}`))
	}))
	defer server.Close()
	service, err := api.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
	require.NoError(t, err)
	c := &connection{projectID: "billing", service: service}
	page, err := c.ListTableMetadata(context.Background(), "source", "data", 7, "opaque+/=")
	require.NoError(t, err)
	require.Empty(t, page.Tables)
	require.Equal(t, "next", page.NextPageToken)
	require.Equal(t, 1, calls)
	require.Equal(t, "billing", c.projectID)
	for _, code := range []int{403, 404} {
		status = code
		_, err = c.ListTableMetadata(context.Background(), "source", "data", 7, "opaque+/=")
		var cause *googleapi.Error
		require.ErrorAs(t, err, &cause)
		require.Equal(t, code, cause.Code)
	}
	before := calls
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.ListTableMetadata(ctx, "source", "data", 7, "")
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.ListTableMetadata(context.Background(), "source", "", 7, "")
	require.Error(t, err)
	_, err = c.ListTableMetadata(context.Background(), "source", "data", 0, "")
	require.Error(t, err)
	require.Equal(t, before, calls)
}
