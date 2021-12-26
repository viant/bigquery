package param

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/bigquery/v2"
	"testing"
)

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

		expect := &bigquery.QueryParameter{}
		_ = json.Unmarshal([]byte(testCase.expect), expect)

		if !assert.EqualValues(t, expect, queryParam, testCase.description) {
			actual, _ := json.Marshal(queryParam)
			fmt.Println(string(actual))
		}
	}

}
