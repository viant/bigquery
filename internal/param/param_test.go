package param

import (
	"encoding/json"
	"math/big"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParam_QueryParameterNil(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{"untyped", nil},
		{"struct pointer", (*struct{ ID int })(nil)},
		{"pointer to pointer", (**int)(nil)},
		{"interface pointer", (*interface{})(nil)},
		{"slice pointer", (*[]int)(nil)},
		{"unsupported slice", []interface{}(nil)},
		{"unsupported non-nil slice", []interface{}{1}},
		{"map", map[string]int(nil)},
		{"channel", (chan int)(nil)},
		{"function", (func())(nil)},
		{"unsafe pointer", unsafe.Pointer(nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := New("key", tt.value).QueryParameter()
			require.Error(t, err)
			require.Nil(t, actual)
			if tt.value == nil {
				require.EqualError(t, err, "cannot encode untyped NULL query parameter: explicit BigQuery type required")
			}
		})
	}
}

func TestParam_QueryParameterTypedNil(t *testing.T) {
	for _, tt := range []struct {
		value    interface{}
		typeName string
	}{
		{(*bool)(nil), "BOOL"},
		{(*int)(nil), "INT64"},
		{(*int8)(nil), "INT64"},
		{(*int16)(nil), "INT64"},
		{(*int32)(nil), "INT64"},
		{(*int64)(nil), "INT64"},
		{(*uint)(nil), "INT64"},
		{(*uint8)(nil), "INT64"},
		{(*uint16)(nil), "INT64"},
		{(*uint32)(nil), "INT64"},
		{(*uint64)(nil), "INT64"},
		{(*float32)(nil), "FLOAT64"},
		{(*float64)(nil), "FLOAT64"},
		{(*string)(nil), "STRING"},
		{(*time.Time)(nil), "TIMESTAMP"},
		{(*big.Rat)(nil), "BIGNUMERIC"},
		{[]byte(nil), "BYTES"},
	} {
		t.Run(reflect.TypeOf(tt.value).String(), func(t *testing.T) {
			actual, err := New("key", tt.value).QueryParameter()
			require.NoError(t, err)
			encoded, err := json.Marshal(actual)
			require.NoError(t, err)
			require.JSONEq(t, `{"name":"key","parameterType":{"type":"`+tt.typeName+`"},"parameterValue":{}}`, string(encoded))
		})
	}
}

func TestParam_QueryParameterNilSlice(t *testing.T) {
	actual, err := New("key", []int(nil)).QueryParameter()
	require.NoError(t, err)
	empty, err := New("key", []int{}).QueryParameter()
	require.NoError(t, err)
	require.Equal(t, empty, actual)
	require.Equal(t, "ARRAY", actual.ParameterType.Type)
	require.Equal(t, "INT64", actual.ParameterType.ArrayType.Type)
}

func TestParam_QueryParameterZeroPointers(t *testing.T) {
	for _, tt := range []struct {
		value interface{}
		want  string
	}{
		{new(int), `{"type":"INT64"},"parameterValue":{"value":"0"}`},
		{new(bool), `{"type":"BOOL"},"parameterValue":{"value":"false"}`},
		{new(string), `{"type":"STRING"},"parameterValue":{"value":""}`},
	} {
		t.Run(reflect.TypeOf(tt.value).String(), func(t *testing.T) {
			actual, err := New("key", tt.value).QueryParameter()
			require.NoError(t, err)
			encoded, err := json.Marshal(actual)
			require.NoError(t, err)
			require.JSONEq(t, `{"name":"key","parameterType":`+tt.want+`}`, string(encoded))
		})
	}
}

func TestParam_QueryParameterNew(t *testing.T) {
	var testCases = []struct {
		description string
		name        string
		value       interface{}
		expect      string
		skip        bool
	}{
		{
			description: "int param",
			name:        "p1",
			value:       101,
			expect:      `{"name":"p1","parameterType":{"type":"INT64"},"parameterValue":{"value":"101"}}`,
		},
		{
			description: "string param",
			name:        "p1",
			value:       "test",
			expect:      `{"name":"p1","parameterType":{"type":"STRING"},"parameterValue":{"value":"test"}}`,
		},
		{
			description: "float64 param",
			name:        "p1",
			value:       4.3,
			expect:      `{"name":"p1","parameterType":{"type":"FLOAT64"},"parameterValue":{"value":"4.3"}}`,
		},
		{
			description: "[]int param",
			name:        "p1",
			value:       []int{1, 100},
			expect:      `{"name":"p1","parameterType":{"arrayType":{"type":"INT64"},"type":"ARRAY"},"parameterValue":{"arrayValues":[{"value":"1"},{"value":"100"}]}}`,
		},
		{
			description: "struct param",
			name:        "p1",
			value: struct {
				ID   int
				Name string
			}{ID: 1, Name: "test"},
			expect: `{"name":"p1","parameterType":{"structTypes":[{"name":"ID","type":{"type":"INT64"}},{"name":"Name","type":{"type":"STRING"}}]},"parameterValue":{"structValues":{"ID":{"value":"1"},"Name":{"value":"test"}}}}`,
		},
		{
			description: "struct with slice param and pointer",
			name:        "p1",
			value: struct {
				ID     int
				Name   string
				Splits []float32
				Active *bool
			}{ID: 1, Name: "test", Splits: []float32{123.3, 3}},
			expect: `{"name":"p1","parameterType":{"structTypes":[{"name":"ID","type":{"type":"INT64"}},{"name":"Name","type":{"type":"STRING"}},{"name":"Splits","type":{"arrayType":{"type":"FLOAT64"},"type":"ARRAY"}},{"name":"Active","type":{"type":"BOOL"}}]},"parameterValue":{"structValues":{"Active":{"value":"false"},"ID":{"value":"1"},"Name":{"value":"test"},"Splits":{"arrayValues":[{"value":"123.30000305175781"},{"value":"3"}]}}}}`,
		},
	}

	for _, testCase := range testCases {
		param := New(testCase.name, testCase.value)
		queryParam, err := param.QueryParameter()
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		if testCase.skip {
			continue
		}

		actual, err := json.Marshal(queryParam)
		require.NoError(t, err, testCase.description)
		assert.JSONEq(t, testCase.expect, string(actual), testCase.description)
	}

}
