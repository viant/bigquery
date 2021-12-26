package decoder

import (
	"crypto/sha256"
	"github.com/francoispqt/gojay"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/bigquery/v2"
	"reflect"
	"testing"
	"time"
)

func sha256Hash(text string) []byte {
	hasher := sha256.New()
	hasher.Write([]byte(text))
	return hasher.Sum(nil)
}

func TestNew(t *testing.T) {

	type Foo struct {
		ID int
	}

	type Participant struct {
		Name string
		Splits []float64
	}

	var testCases = []struct {
		description string
		JSON        string
		schema      *bigquery.TableSchema
		types       []reflect.Type
		expect      []interface{}
	}{

		{
			description: "base types",
			schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
				{
					Name: "f1",
					Type: "INTEGER",
					Mode: "NULLABLE",
				},
				{
					Name: "f2",
					Type: "STRING",
					Mode: "NULLABLE",
				},
				{
					Name: "f3",
					Type: "BYTES",
					Mode: "NULLABLE",
				},
				{
					Name: "f5",
					Type: "TIMESTAMP",
					Mode: "NULLABLE",
				},
				{
					Name: "f6",
					Type: "BOOLEAN",
					Mode: "NULLABLE",
				},
				{
					Name: "f7",
					Type: "INTEGER",
					Mode: "NULLABLE",
				},
			}},
			JSON: `{"f":[{"v":"1"},{"v":"TEXT"},{"v":"ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="},{"v":"1.63966552385427E9"},{"v":"true"},{"v":null}]}`,
			types: []reflect.Type{
				reflect.TypeOf(0),
				reflect.TypeOf(""),
				reflect.TypeOf([]byte{}),
				reflect.TypeOf(time.Time{}),
				reflect.TypeOf(true),
				reflect.TypeOf(0),
			},
			expect: []interface{}{
				1,
				"TEXT",
				sha256Hash("abc"),
				time.Unix(0, int64(float64(1.63966552385427e9)*1000000)*int64(time.Microsecond)),
			},
		},
		{
			description: "base repeated",
			schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
				{
					Name: "f1",
					Type: "INTEGER",
					Mode: "NULLABLE",
				},
				{
					Name: "f2",
					Type: "INTEGER",
					Mode: "REPEATED",
				}}},
			types: []reflect.Type{
				reflect.TypeOf(0),
				reflect.TypeOf([]int{}),
			},
			JSON: `{"f":[{"v":"0"},{"v":[{"v":"1"},{"v":"2"},{"v":"3"}]}]}`,
			expect: []interface{}{
				0,
				[]int{1, 2, 3},
			},
		},
		{
			description: "struct",
			schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
				{
					Name: "race",
					Type: "STRING",
					Mode: "NULLABLE",
				},
				{
					Name: "participant",
					Type: "RECORD",
					Mode: "NULLABLE",
					Fields: []*bigquery.TableFieldSchema{
						{
							Name: "name",
							Type: "STRING",
							Mode: "NULLABLE",
						},
						{
							Name: "splits",
							Type: "FLOAT",
							Mode: "REPEATED",
						},
					}}}},
			types: []reflect.Type{
				reflect.TypeOf(""),
				reflect.TypeOf(Participant{}),
			},
			JSON: `{"f":[{"v":"800M"},{"v":{"f":[{"v":"Rudisha"},{"v":[{"v":"23.4"},{"v":"26.3"}]}]}}]}`,
			expect: []interface{}{
				"800M",
				Participant{Name: "Rudisha", Splits: []float64{23.4, 26.3}},
			},
		},
		{
			description: "nil slice",
			schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
				{
					Name: "f1",
					Type: "INTEGER",
					Mode: "REPEATED",
				},
				{
					Name: "f2",
					Type: "STRING",
					Mode: "NULLABLE",
				},
			}},
			JSON: `{"f":[{"v":[]},{"v":"test"}]}`,
			types: []reflect.Type{
				reflect.TypeOf([]int{}),
				reflect.TypeOf(""),

			},
			expect: []interface{}{
				[]int(nil),
				"test",
			},
		},
		{
			description: "nil struct",
			schema: &bigquery.TableSchema{Fields: []*bigquery.TableFieldSchema{
				{
					Name: "f1",
					Type: "RECORD",
					Mode: "NULLABLE",
					Fields: []*bigquery.TableFieldSchema{
						{
							Name: "ID",
							Type: "INTEGER",
							Mode: "NULLABLE",
						},
					},
				},
				{
					Name: "f2",
					Type: "STRING",
					Mode: "NULLABLE",
				},
			}},
			JSON: `{"f":[{"v":null},{"v":"test"}]}`,
			types: []reflect.Type{
				reflect.TypeOf(&Foo{}),
				reflect.TypeOf(""),

			},
			expect: []interface{}{
				(*Foo)(nil),
				"test",
			},
		},
	}

	for _, testCase := range testCases {
		aNew, err := New(testCase.types, testCase.schema)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		var values = make([]interface{}, len(testCase.types))
		for i, t := range testCase.types {
			ptr := reflect.New(t)
			values[i] = ptr.Interface()
		}
		decoder := aNew(values)
		err = gojay.UnmarshalJSONObject([]byte(testCase.JSON), decoder)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		for i := range testCase.expect {
			actual := reflect.ValueOf(values[i]).Elem().Interface()
			assert.Equal(t, testCase.expect[i], actual, testCase.description)
		}

		values = make([]interface{}, len(testCase.types))
		for i, t := range testCase.types {
			ptr := reflect.New(t)
			values[i] = ptr.Interface()
		}
		decoder.SetValues(values)

		//test decoder reuse
		err = gojay.UnmarshalJSONObject([]byte(testCase.JSON), decoder)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		for i := range testCase.expect {
			actual := reflect.ValueOf(values[i]).Elem().Interface()
			assert.Equal(t, testCase.expect[i], actual, testCase.description)
		}
	}

}
