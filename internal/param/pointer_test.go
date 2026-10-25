package param

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNullablePointerChains(t *testing.T) {
	text := "value"
	p := &text
	pp := &p
	var missing *string
	var outer **string
	for _, tc := range []struct {
		value any
		want  string
	}{{pp, "value"}, {&missing, ""}, {outer, ""}} {
		got, err := New("p", tc.value).QueryParameter()
		require.NoError(t, err)
		require.Equal(t, "STRING", got.ParameterType.Type)
		require.Equal(t, tc.want, got.ParameterValue.Value)
	}
	number := int64(9007199254741993)
	np := &number
	got, err := New("p", &np).QueryParameter()
	require.NoError(t, err)
	require.Equal(t, "9007199254741993", got.ParameterValue.Value)
}
