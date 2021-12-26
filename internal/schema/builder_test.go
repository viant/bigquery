package schema

import (
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/bigquery/v2"
	"reflect"
	"testing"
)

func TestBuildType(t *testing.T) {

	var testCases = []struct {
		description string
		bigquery.TableSchema
		expect []reflect.Type
	}{
		{
			description: "basic types",
			TableSchema: bigquery.TableSchema{
				Fields: []*bigquery.TableFieldSchema{
					{
						Name: "f1",
						Type: "INTEGER",
						Mode: "NULLABLE",
					},
					{
						Name: "f2",
						Type: "STRING",
					},
					{
						Name: "f3",
						Type: "BOOL",
						Mode: "NULLABLE",
					},
				},
			},
			expect: []reflect.Type{
				intType,
				stringType,
				boolType,
			},
		},
		{
			description: "repeated types",
			TableSchema: bigquery.TableSchema{
				Fields: []*bigquery.TableFieldSchema{
					{
						Name: "params",
						Type: "RECORD",
						Mode: "REPEATED",
						Fields: []*bigquery.TableFieldSchema{
							{
								Name: "key",
								Type: "STRING",
							},
							{
								Name: "value",
								Type: "STRING",
								Mode: "NULLABLE",
							},
						},
					},
				},
			},
			expect: []reflect.Type{
				reflect.SliceOf(reflect.StructOf([]reflect.StructField{
					{Name: "Key", Type: stringType, Tag: `json:"key"`},
					{Name: "Value", Type: stringType, Tag: `json:"value,omitempty"`},
				})),
			},
		},
		{
			description: "nested & repeated types",
			TableSchema: bigquery.TableSchema{
				Fields: []*bigquery.TableFieldSchema{
					{
						Name: "Request",
						Type: "RECORD",
						Mode: "NULLABLE",
						Fields: []*bigquery.TableFieldSchema{
							{
								Name: "ts",
								Type: "TIMESTAMP",
								Mode: "NULLABLE",
							},
							{
								Name: "ip",
								Type: "STRING",
								Mode: "NULLABLE",
							},
						},
					},
					{
						Name: "params",
						Type: "RECORD",
						Mode: "REPEATED",
						Fields: []*bigquery.TableFieldSchema{
							{
								Name: "key",
								Type: "STRING",
							},
							{
								Name: "value",
								Type: "STRING",
								Mode: "NULLABLE",
							},
						},
					},
				},
			},
			expect: []reflect.Type{
				reflect.StructOf([]reflect.StructField{
					{Name: "Ts", Type: timeTypePtr, Tag: `json:"ts,omitempty"`},
					{Name: "Ip", Type: stringType, Tag: `json:"ip,omitempty"`},
				}),
				reflect.SliceOf(reflect.StructOf([]reflect.StructField{
					{Name: "Key", Type: stringType, Tag: `json:"key"`},
					{Name: "Value", Type: stringType, Tag: `json:"value,omitempty"`},
				})),
			},
		},
	}

	for _, testCase := range testCases {
		actual, err := BuildSchemaTypes(&testCase.TableSchema)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		if !assertTypes(t, testCase.expect, actual, testCase.description) {
		}
	}
}

func assertTypes(t *testing.T, expect []reflect.Type, actual []reflect.Type, description string) bool {
	if !assert.Equal(t, len(expect), len(actual), description) {
		return false
	}
	for i := range expect {
		if !assertType(t, expect[i], actual[i], description) {
			return false
		}
	}
	return true
}

func assertType(t *testing.T, expect reflect.Type, actual reflect.Type, description string) bool {

	if !assert.Equal(t, expect.Kind(), actual.Kind(), description) {
		return false
	}
	switch expect.Kind() {
	case reflect.Struct:
		if !assert.Equal(t, expect.NumField(), actual.NumField(), description) {
			return false
		}
		for i := 0; i < expect.NumField(); i++ {
			expectField := expect.Field(i)
			actualField := actual.Field(i)
			if !assertFields(t, expectField, actualField, description) {
				return false
			}
		}
	case reflect.Slice:
		return assertType(t, expect.Elem(), actual.Elem(), description)
	default:
		return assert.EqualValues(t, expect, actual, description)
		//	continue
	}
	return true
}

func assertFields(t *testing.T, expect reflect.StructField, actual reflect.StructField, description string) bool {
	if !assert.Equal(t, expect.Name, actual.Name, description) {
		return false
	}
	if !assert.Equal(t, expect.Tag, actual.Tag, description) {
		return false
	}
	assertType(t, expect.Type, actual.Type, description)
	return true
}
