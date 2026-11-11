package bigquery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// REQUIRED is valid native schema metadata even when a zero-row query returns
// no rows. Decoder preparation must not require nullable pointer destinations.
func TestRequiredScalarSchemaAndRows(t *testing.T) {
	for _, zero := range []bool{true, false} {
		t.Run(fmt.Sprintf("zero_rows_%v", zero), func(t *testing.T) {
			var calls atomic.Int32
			query := "SELECT * FROM fixture"
			if zero {
				query += " WHERE 1=0"
			}
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				require.Equal(t, "/projects/fixture-project/queries", r.URL.Path)
				var input struct{ Query string }
				require.NoError(t, json.NewDecoder(r.Body).Decode(&input))
				require.Equal(t, query, input.Query)
				w.Header().Set("Content-Type", "application/json")
				rows := `[]`
				total := "0"
				if !zero {
					rows = `[{"f":[{"v":"7"},{"v":"sample"},{"v":"true"},{"v":"1.25"},{"v":"AQI="},{"v":"2026-01-02"},{"v":null}]},{"f":[{"v":"8"},{"v":"next"},{"v":"false"},{"v":"2.5"},{"v":"AwQ="},{"v":"2026-01-03"},{"v":"42"}]}]`
					total = "2"
				}
				fmt.Fprintf(w, `{"jobComplete":true,"jobReference":{"projectId":"fixture-project","jobId":"query"},"schema":{"fields":[{"name":"id","type":"INTEGER","mode":"REQUIRED"},{"name":"label","type":"STRING","mode":"REQUIRED"},{"name":"active","type":"BOOLEAN","mode":"REQUIRED"},{"name":"score","type":"FLOAT","mode":"REQUIRED"},{"name":"data","type":"BYTES","mode":"REQUIRED"},{"name":"day","type":"DATE","mode":"REQUIRED"},{"name":"optional","type":"INTEGER","mode":"NULLABLE"}]},"totalRows":%q,"rows":%s}`, total, rows)
			}))
			defer provider.Close()
			db, err := sql.Open("bigquery", "bigquery://fixture-project/dataset?emulator=true&queryMode=sync&endpoint="+url.QueryEscape(provider.URL+"/"))
			require.NoError(t, err)
			defer db.Close()
			rows, err := db.QueryContext(context.Background(), query)
			require.NoError(t, err)
			defer rows.Close()
			columns, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, columns, 7)
			for i, column := range columns {
				nullable, known := column.Nullable()
				require.True(t, known)
				require.Equal(t, i == 6, nullable, column.Name())
			}
			count := 0
			for rows.Next() {
				var id int
				var label string
				var active bool
				var score float64
				var data []byte
				var day time.Time
				var optional sql.NullInt64
				require.NoError(t, rows.Scan(&id, &label, &active, &score, &data, &day, &optional))
				if count == 0 {
					require.Equal(t, 7, id)
					require.Equal(t, "sample", label)
					require.True(t, active)
					require.Equal(t, 1.25, score)
					require.Equal(t, []byte{1, 2}, data)
					require.Equal(t, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), day)
					require.False(t, optional.Valid)
				} else {
					require.Equal(t, 8, id)
					require.Equal(t, "next", label)
					require.False(t, active)
					require.Equal(t, 2.5, score)
					require.Equal(t, []byte{3, 4}, data)
					require.Equal(t, int64(42), optional.Int64)
					require.True(t, optional.Valid)
				}
				count++
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			require.EqualValues(t, 1, calls.Load())
			if zero {
				require.Zero(t, count)
			} else {
				require.Equal(t, 2, count)
			}
		})
	}
}
