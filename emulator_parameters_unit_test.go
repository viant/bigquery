package bigquery

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	bq "google.golang.org/api/bigquery/v2"
)

func TestEmulatorParameterTokens(t *testing.T) {
	for _, tc := range []struct {
		query, want string
		count       int
		fail        bool
	}{
		{query: "SELECT ?, ?", want: "SELECT @viant_emulator_p1, @viant_emulator_p2", count: 2},
		{query: "SELECT '?', \"?\", `?`, ? -- ? @ignored\n/* ? @ignored */ # ?\n", want: "SELECT '?', \"?\", `?`, @viant_emulator_p1 -- ? @ignored\n/* ? @ignored */ # ?\n", count: 1},
		{query: "SELECT '''?'?''', \"\"\"?\"?\"\"\", ?", want: "SELECT '''?'?''', \"\"\"?\"?\"\"\", @viant_emulator_p1", count: 1},
		{query: "SELECT 'it''s ?', 'escaped \\'?', ?", want: "SELECT 'it''s ?', 'escaped \\'?', @viant_emulator_p1", count: 1},
		{query: "SELECT @@dataset_project_id, ?", want: "SELECT @@dataset_project_id, @viant_emulator_p1", count: 1},
		{query: "SELECT @value, @value", want: "SELECT @value, @value"},
		{query: "SELECT ?, @viant_emulator_p1", fail: true},
		{query: "SELECT 'unterminated ?", want: "SELECT 'unterminated ?"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			got, names, err := emulatorParameters(tc.query)
			if tc.fail {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.Len(t, names, tc.count)
		})
	}
}
func TestEmulatorBindingIsConnectionScoped(t *testing.T) {
	query := "SELECT ? AS value -- ignored ? @comment"
	for _, emulator := range []bool{false, true} {
		conn := &connection{cfg: &Config{Emulator: emulator}}
		prepared, err := conn.PrepareContext(context.Background(), query)
		require.NoError(t, err)
		stmt := prepared.(*Statement)
		params := []*bq.QueryParameter{{ParameterType: &bq.QueryParameterType{Type: "STRING"}, ParameterValue: &bq.QueryParameterValue{Value: "value"}}}
		require.NoError(t, stmt.bindParameters(params))
		if emulator {
			require.Equal(t, 1, stmt.NumInput())
			require.Equal(t, "NAMED", stmt.job.Configuration.Query.ParameterMode)
			require.True(t, strings.Contains(stmt.job.Configuration.Query.Query, "@viant_emulator_p1"))
			require.Equal(t, "viant_emulator_p1", stmt.job.Configuration.Query.QueryParameters[0].Name)
		} else {
			require.Equal(t, query, stmt.job.Configuration.Query.Query)
			require.Empty(t, stmt.job.Configuration.Query.ParameterMode)
			require.Empty(t, stmt.job.Configuration.Query.QueryParameters[0].Name)
		}
		require.Empty(t, params[0].Name, "caller-owned parameters were modified")
		if emulator {
			require.Error(t, stmt.bindParameters(nil))
			require.Error(t, stmt.bindParameters([]*bq.QueryParameter{{Name: "other"}}))
		}
	}
}
