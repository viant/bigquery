package bigquery

import (
	"database/sql/driver"
	"encoding/json"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValues_QueryParameterNil(t *testing.T) {
	var key *int
	converted, err := driver.DefaultParameterConverter.ConvertValue(key)
	require.NoError(t, err)
	require.Nil(t, converted)

	for _, tt := range []struct {
		name  string
		value driver.Value
	}{
		{"untyped", nil},
		{"converted pointer", converted},
		{"unsupported pointer", (**int)(nil)},
		{"unsupported slice", []interface{}(nil)},
		{"unsupported struct pointer", (*struct{ ID int })(nil)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, prefix := range []Values{nil, {int64(123), "private-bound-value"}} {
				args := append(prefix, tt.value)
				actual, err := args.QueryParameter()
				require.Error(t, err)
				require.Nil(t, actual)
				require.Contains(t, err.Error(), "query parameter at position ")
				require.NotContains(t, err.Error(), "private-bound-value")
				if len(prefix) == 0 {
					require.Contains(t, err.Error(), "position 1:")
				} else {
					require.Contains(t, err.Error(), "position 3:")
				}

				named := make(NamedValues, len(args))
				for i, value := range args {
					named[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
				}
				actual, namedErr := named.QueryParameter()
				require.Nil(t, actual)
				require.EqualError(t, namedErr, err.Error())
			}
		})
	}
}

func TestValues_QueryParameterTypedNil(t *testing.T) {
	for _, tt := range []struct {
		value    driver.Value
		typeName string
	}{
		{(*int)(nil), "INT64"},
		{(*bool)(nil), "BOOL"},
		{(*float64)(nil), "FLOAT64"},
		{(*string)(nil), "STRING"},
		{(*time.Time)(nil), "TIMESTAMP"},
		{(*big.Rat)(nil), "BIGNUMERIC"},
		{[]byte(nil), "BYTES"},
	} {
		t.Run(reflect.TypeOf(tt.value).String(), func(t *testing.T) {
			actual, err := (Values{int64(123), tt.value}).QueryParameter()
			require.NoError(t, err)
			named, err := (NamedValues{
				{Ordinal: 1, Value: int64(123)},
				{Ordinal: 2, Value: tt.value},
			}).QueryParameter()
			require.NoError(t, err)
			require.Equal(t, actual, named)
			encoded, err := json.Marshal(actual[1])
			require.NoError(t, err)
			require.JSONEq(t, `{"parameterType":{"type":"`+tt.typeName+`"},"parameterValue":{}}`, string(encoded))
		})
	}
}

func TestNamedValues_QueryParameterErrorContext(t *testing.T) {
	for _, tt := range []struct {
		name string
		arg  driver.NamedValue
		want string
	}{
		{"name", driver.NamedValue{Name: "key", Ordinal: 7}, `query parameter "key":`},
		{"ordinal", driver.NamedValue{Ordinal: 7}, "query parameter at position 7:"},
		{"fallback", driver.NamedValue{}, "query parameter at position 2:"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := (NamedValues{{Ordinal: 1, Value: "private-bound-value"}, tt.arg}).QueryParameter()
			require.Nil(t, actual)
			require.ErrorContains(t, err, tt.want)
			require.NotContains(t, err.Error(), "private-bound-value")
		})
	}
}

func TestValues_QueryParameterZeroValues(t *testing.T) {
	args := Values{int64(0), false, ""}
	actual, err := args.QueryParameter()
	require.NoError(t, err)
	named, err := (NamedValues{
		{Ordinal: 1, Value: int64(0)},
		{Ordinal: 2, Value: false},
		{Ordinal: 3, Value: ""},
	}).QueryParameter()
	require.NoError(t, err)
	require.Equal(t, actual, named)
	encoded, err := json.Marshal(actual)
	require.NoError(t, err)
	require.JSONEq(t, `[
		{"parameterType":{"type":"INT64"},"parameterValue":{"value":"0"}},
		{"parameterType":{"type":"BOOL"},"parameterValue":{"value":"false"}},
		{"parameterType":{"type":"STRING"},"parameterValue":{"value":""}}
	]`, string(encoded))
}
