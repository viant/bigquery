package bigquery

import (
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRowsNullableBytesDatabaseSQL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != "POST" || r.URL.Path != "/projects/p/queries" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
			return
		}
		io.WriteString(w, `{"jobComplete":true,"jobReference":{"projectId":"p","jobId":"j"},"schema":{"fields":[{"name":"sketch","type":"BYTES","mode":"NULLABLE"}]},"totalRows":"4","rows":[{"f":[{"v":"AAH/"}]},{"f":[{"v":null}]},{"f":[{"v":""}]},{"f":[{"v":"Ag=="}]}]}`)
	}))
	defer server.Close()
	db, err := sql.Open("bigquery", "bigquery://p/d?emulator=true&queryMode=sync&endpoint="+url.QueryEscape(server.URL+"/"))
	require.NoError(t, err)
	defer db.Close()
	expected := []any{[]byte{0, 1, 255}, nil, []byte{}, []byte{2}}
	for _, asBytes := range []bool{false, true} {
		rows, err := db.Query("SELECT sketch FROM records")
		require.NoError(t, err)
		index := 0
		for rows.Next() {
			require.Less(t, index, len(expected))
			if asBytes {
				var actual []byte
				require.NoError(t, rows.Scan(&actual))
				if expected[index] == nil {
					require.Nil(t, actual)
				} else {
					require.NotNil(t, actual)
					require.Equal(t, expected[index], actual)
				}
			} else {
				var actual any
				require.NoError(t, rows.Scan(&actual))
				require.Equal(t, expected[index], actual)
			}
			index++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, len(expected), index)
		require.NoError(t, rows.Close())
	}
}
