package bigquery

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// JSON values travel as JSON text inside the REST cell's v string. Preserve the
// text (including large integers) and distinguish SQL NULL from JSON null.
func TestRowsJSONDatabaseSQL(t *testing.T) {
	values := []any{`{"attribute_id":[9007199254741993],"zero":0,"enabled":false}`, nil, `null`, `"hello"`, `[]`, `false`, `0`}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "POST", r.Method)
		require.Equal(t, "/projects/p/queries", r.URL.Path)
		rows := []any{}
		for _, value := range values {
			rows = append(rows, map[string]any{"f": []any{map[string]any{"v": value}}})
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jobComplete": true, "jobReference": map[string]string{"projectId": "p", "jobId": "j"}, "schema": map[string]any{"fields": []any{map[string]string{"name": "attributes", "type": "JSON", "mode": "NULLABLE"}}}, "totalRows": "7", "rows": rows}))
	}))
	defer server.Close()
	db, err := sql.Open("bigquery", "bigquery://p/d?emulator=true&queryMode=sync&endpoint="+url.QueryEscape(server.URL+"/"))
	require.NoError(t, err)
	defer db.Close()
	for _, asBytes := range []bool{false, true} {
		rows, err := db.Query("SELECT attributes FROM records")
		require.NoError(t, err)
		types, err := rows.ColumnTypes()
		require.NoError(t, err)
		require.Equal(t, "JSON", types[0].DatabaseTypeName())
		index := 0
		for rows.Next() {
			require.Less(t, index, len(values))
			if asBytes {
				var value []byte
				require.NoError(t, rows.Scan(&value))
				if values[index] == nil {
					require.Nil(t, value)
				} else {
					require.Equal(t, values[index], string(value))
				}
			} else {
				var value any
				require.NoError(t, rows.Scan(&value))
				require.Equal(t, values[index], value)
			}
			index++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, len(values), index)
		require.NoError(t, rows.Close())
	}
}

func TestJSONParameterEmulator(t *testing.T) {
	endpoint := os.Getenv("BIGQUERY_JSON_EMULATOR_ENDPOINT")
	if endpoint == "" {
		t.Skip("requires owned local emulator")
	}
	u, err := url.Parse(endpoint)
	require.NoError(t, err)
	require.Equal(t, "http", u.Scheme)
	require.Equal(t, "127.0.0.1", u.Hostname())
	db, err := sql.Open("bigquery", "bigquery://viant-imp-partners/uam_prod?emulator=true&queryMode=sync&endpoint="+url.QueryEscape(endpoint))
	require.NoError(t, err)
	defer db.Close()
	raw := json.RawMessage(`{"id":9007199254741993,"enabled":false,"zero":0,"empty":[],"missing":null}`)
	var got string
	require.NoError(t, db.QueryRow("SELECT ? AS payload", raw).Scan(&got))
	require.JSONEq(t, string(raw), got)
	require.Contains(t, got, "9007199254741993")
	var sqlNull any
	require.NoError(t, db.QueryRow("SELECT ? AS payload", json.RawMessage(nil)).Scan(&sqlNull))
	require.Nil(t, sqlNull)
	var jsonNull string
	require.NoError(t, db.QueryRow("SELECT ? AS payload", json.RawMessage("null")).Scan(&jsonNull))
	require.Equal(t, "null", jsonNull)
}
