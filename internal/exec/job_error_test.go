package exec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

func TestCompletedJobRetainsStructuredReason(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":{"state":"DONE","errorResult":{"reason":"accessDenied","message":"CANARY_SECRET"},"errors":[{"reason":"accessDenied","message":"CANARY_QUERY"}]}}`))
	}))
	defer server.Close()
	service, err := bigquery.NewService(context.Background(), option.WithEndpoint(server.URL+"/"), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	_, err = WaitForJobCompletion(context.Background(), service, "project", "US", "job")
	var native interface{ BigQueryReason() string }
	if !errors.As(fmt.Errorf("query failed: %w", err), &native) || native.BigQueryReason() != "accessDenied" {
		t.Fatalf("structured native cause lost: %v", err)
	}
	if !strings.Contains(err.Error(), "CANARY_SECRET") {
		t.Fatal("trusted caller lost original native text")
	}
	data, marshalErr := json.Marshal(err)
	if marshalErr != nil || strings.Contains(string(data), "CANARY") {
		t.Fatalf("private native fields serialized: %s %v", data, marshalErr)
	}
}

func TestJobErrorRetainsFullProviderDetailWithoutDuplicateMessages(t *testing.T) {
	status := &bigquery.JobStatus{ErrorResult: &bigquery.ErrorProto{Reason: "invalidQuery", Message: "Query failed at [1:23]", Location: "query"}, Errors: []*bigquery.ErrorProto{{Message: "Query failed at [1:23]"}, nil, {Message: "Unknown field in source"}}}
	err := JobError(&bigquery.Job{Status: status})
	var detail interface {
		BigQueryMessage() string
		BigQueryLocation() string
	}
	if !errors.As(err, &detail) || detail.BigQueryMessage() != "Query failed at [1:23]\nUnknown field in source" || detail.BigQueryLocation() != "query" {
		t.Fatalf("provider detail lost: %v", err)
	}
	raw, e := json.Marshal(err)
	if e != nil || string(raw) != "{}" {
		t.Fatalf("private fields serialized: %s %v", raw, e)
	}
	if JobError(nil) != nil || JobError(&bigquery.Job{}) != nil {
		t.Fatal("successful job acquired an error")
	}
}
