package bigquery

import (
	"context"
	"github.com/stretchr/testify/require"
	api "google.golang.org/api/bigquery/v2"
	"testing"
)

func TestEmulatorParametersPreserveQuotedTextAndComments(t *testing.T) {
	query := "SELECT '?', \"?\", `a?b`, '''?''', r'''?''', ? -- ?\n /* ? */ WHERE id=? # ?\n"
	got, names, err := emulatorParameters(query)
	require.NoError(t, err)
	require.Equal(t, "SELECT '?', \"?\", `a?b`, '''?''', r'''?''', @viantPosition1 -- ?\n /* ? */ WHERE id=@viantPosition2 # ?\n", got)
	require.Equal(t, []string{"viantPosition1", "viantPosition2"}, names)
	_, _, err = emulatorParameters("SELECT @named, ?")
	require.Error(t, err)
	_, _, err = emulatorParameters("SELECT 'unfinished ?")
	require.Error(t, err)
	_, _, err = emulatorParameters("SELECT /* unfinished ?")
	require.Error(t, err)
}
func TestEmulatorBindingsAreTypedAndConnectionScoped(t *testing.T) {
	query := "SELECT DATE(tstamp, ?), COUNT(DISTINCT CASE WHEN tstamp < TIMESTAMP(DATE(?, ?), ?) THEN event_sid END) FROM events"
	for _, emulator := range []bool{false, true} {
		conn := &connection{cfg: &Config{Emulator: emulator}}
		prepared, err := conn.PrepareContext(context.Background(), query)
		require.NoError(t, err)
		stmt := prepared.(*Statement)
		params := []*api.QueryParameter{{ParameterType: &api.QueryParameterType{Type: "STRING"}, ParameterValue: &api.QueryParameterValue{Value: "UTC"}}, {ParameterType: &api.QueryParameterType{Type: "TIMESTAMP"}, ParameterValue: &api.QueryParameterValue{Value: "2026-10-16T00:00:00Z"}}, {ParameterType: &api.QueryParameterType{Type: "STRING"}, ParameterValue: &api.QueryParameterValue{Value: "UTC"}}, {ParameterType: &api.QueryParameterType{Type: "STRING"}, ParameterValue: &api.QueryParameterValue{Value: "UTC"}}}
		require.NoError(t, stmt.bindParameters(params))
		require.Empty(t, params[0].Name, "caller parameters must not be mutated")
		if emulator {
			require.Equal(t, "NAMED", stmt.job.Configuration.Query.ParameterMode)
			require.Equal(t, "viantPosition2", stmt.job.Configuration.Query.QueryParameters[1].Name)
			require.Equal(t, "TIMESTAMP", stmt.job.Configuration.Query.QueryParameters[1].ParameterType.Type)
			require.NotContains(t, stmt.job.Configuration.Query.Query, "2026-10-16")
			require.Error(t, stmt.bindParameters(params[:1]))
		} else {
			require.Equal(t, query, stmt.job.Configuration.Query.Query)
			require.Empty(t, stmt.job.Configuration.Query.ParameterMode)
		}
	}
}
