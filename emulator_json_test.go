package bigquery

import (
	"github.com/stretchr/testify/require"
	api "google.golang.org/api/bigquery/v2"
	"testing"
)

func TestEmulatorJSONBindingPreservesSQLAndProduction(t *testing.T) {
	for _, emulator := range []bool{true, false} {
		conn := &connection{cfg: &Config{Emulator: emulator}, projectID: "p"}
		prepared, err := conn.Prepare("SELECT @data, '@data', `@data`, @database -- @data\n/* @data */")
		require.NoError(t, err)
		stmt := prepared.(*Statement)
		param := &api.QueryParameter{Name: "data", ParameterType: &api.QueryParameterType{Type: "JSON"}, ParameterValue: &api.QueryParameterValue{Value: `{"a":0}`}}
		require.NoError(t, stmt.bindParameters([]*api.QueryParameter{param}))
		want := "SELECT @data, '@data', `@data`, @database -- @data\n/* @data */"
		if emulator {
			want = "SELECT PARSE_JSON(@data), '@data', `@data`, @database -- @data\n/* @data */"
		}
		require.Equal(t, want, stmt.job.Configuration.Query.Query)
		require.Equal(t, "JSON", param.ParameterType.Type)
		require.NoError(t, stmt.bindParameters([]*api.QueryParameter{param}))
		require.Equal(t, want, stmt.job.Configuration.Query.Query)
		scalar := *param
		scalar.ParameterType = &api.QueryParameterType{Type: "STRING"}
		require.NoError(t, stmt.bindParameters([]*api.QueryParameter{&scalar}))
		require.NotContains(t, stmt.job.Configuration.Query.Query, "PARSE_JSON")
	}
}
