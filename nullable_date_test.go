package bigquery_test

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "github.com/viant/bigquery"
)

func TestRowsPreserveNullableTemporalValues(t *testing.T) {
	for _, test := range []struct {
		kind  string
		value string
		want  time.Time
	}{
		{kind: "DATE", value: "0001-01-01", want: time.Time{}},
		{kind: "DATETIME", value: "0001-01-01T00:00:00", want: time.Time{}},
		{kind: "TIME", value: "00:00:00", want: time.Date(0, time.January, 1, 0, 0, 0, 0, time.UTC)},
		{kind: "TIMESTAMP", value: "0", want: time.Unix(0, 0).UTC()},
	} {
		for _, mode := range []string{"jobs", "sync"} {
			t.Run(test.kind+"/"+mode, func(t *testing.T) {
				assertNullableTemporalValues(t, mode, test.kind, test.value, test.want)
			})
		}
	}
}

func assertNullableTemporalValues(t *testing.T, mode, kind, value string, want time.Time) {
	t.Helper()
	valueRow := fmt.Sprintf(`{"f":[{"v":%q}]}`, value)
	fixtureRows := []string{valueRow, `{"f":[{"v":null}]}`, `{"f":[{}]}`, valueRow}
	expected := []*time.Time{&want, nil, nil, &want}
	if kind != "TIMESTAMP" {
		// Preserve the existing explicit blank civil-time behavior.
		fixtureRows = append(fixtureRows, `{"f":[{"v":""}]}`)
		expected = append(expected, &time.Time{})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case mode == "jobs" && r.Method == http.MethodPost && r.URL.Path == "/projects/p/jobs":
			fmt.Fprint(w, `{"jobReference":{"projectId":"p","jobId":"j"},"status":{"state":"DONE"}}`)
		case mode == "jobs" && r.Method == http.MethodGet && r.URL.Path == "/projects/p/queries/j",
			mode == "sync" && r.Method == http.MethodPost && r.URL.Path == "/projects/p/queries":
			fmt.Fprintf(w, `{"jobComplete":true,"jobReference":{"projectId":"p","jobId":"j"},"schema":{"fields":[{"name":"date","type":%q,"mode":"NULLABLE"}]},"totalRows":"%d","rows":[%s]}`, kind, len(fixtureRows), strings.Join(fixtureRows, ","))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	dsn := "bigquery://p/d?emulator=true&endpoint=" + url.QueryEscape(server.URL+"/")
	if mode == "sync" {
		dsn += "&queryMode=sync"
	}
	db, err := sql.Open("bigquery", dsn)
	require.NoError(t, err)
	defer db.Close()

	rows, err := db.QueryContext(t.Context(), "SELECT date FROM fixture")
	require.NoError(t, err)
	defer rows.Close()
	for index, want := range expected {
		require.True(t, rows.Next(), "row %d", index)
		var actual *time.Time
		var raw any
		require.NoError(t, rows.Scan(&actual))
		require.NoError(t, rows.Scan(&raw))
		if want == nil {
			require.Nil(t, actual, "row %d", index)
			require.True(t, raw == nil, "row %d exposed raw value %#v", index, raw)
			continue
		}
		require.NotNil(t, actual, "row %d", index)
		require.True(t, actual.Equal(*want), "row %d got %v, want %v", index, actual, want)
		rawTime, isTime := raw.(time.Time)
		require.True(t, isTime && rawTime.Equal(*want), "row %d exposed raw value %#v", index, raw)
	}
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
}
