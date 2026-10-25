package param

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type explicitValue struct {
	kind  string
	value any
}

func (v explicitValue) SQLType() string { return v.kind }
func (v explicitValue) SQLValue() any   { return v.value }
func TestTemporalTypesDoNotCollapseToTimestamp(t *testing.T) {
	value := time.Date(2026, 10, 25, 1, 2, 3, 123456000, time.FixedZone("local", 3600))
	p := &value
	for _, tc := range []struct{ kind, want string }{{"DATE", "2026-10-25"}, {"TIME", "01:02:03.123456"}, {"DATETIME", "2026-10-25 01:02:03.123456"}, {"TIMESTAMP", "2026-10-25T01:02:03.123456+01:00"}} {
		got, err := New("p", explicitValue{tc.kind, &p}).QueryParameter()
		require.NoError(t, err)
		require.Equal(t, tc.kind, got.ParameterType.Type)
		require.Equal(t, tc.want, got.ParameterValue.Value)
		var missing *time.Time
		got, err = New("p", explicitValue{tc.kind, &missing}).QueryParameter()
		require.NoError(t, err)
		require.Equal(t, tc.kind, got.ParameterType.Type)
		require.Empty(t, got.ParameterValue.Value)
	}
	_, err := New("p", explicitValue{"DATETIME", 3}).QueryParameter()
	require.Error(t, err)
	_, err = New("p", explicitValue{"UNKNOWN", value}).QueryParameter()
	require.Error(t, err)
}
