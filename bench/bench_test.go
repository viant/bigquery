package bench

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/bigquery"
	"os"
	"path"
	"reflect"
	"testing"
)

func init() {
	os.Setenv("GCP_PROJECT", "viant-e2e")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path.Join(os.Getenv("HOME"), ".secret/viant-e2e.json"))
}

func Benchmark_Primitive_GCPClient(b *testing.B) {
	b.ReportAllocs()
	benchmark(b, true, 0)
}

func Benchmark_Primitive_SQLDriver(b *testing.B) {
	b.ReportAllocs()
	benchmark(b, false, 0)
}


func Benchmark_Complex_GCPClient(b *testing.B) {
	b.ReportAllocs()
	benchmark(b, true, 1)
}

func Benchmark_Complex_SQLDriver(b *testing.B) {
	b.ReportAllocs()
	benchmark(b, false, 1)
}

type Localized struct {
	Text string
	Language string
	Truncated bool
}


var testCases = []struct {
	description string
	SQL        string
	projection []interface{}
	repeat     int
}{
	{
		description: "primitive types",
		SQL:         "SELECT state,gender,year,name,number FROM `bigquery-public-data.usa_names.usa_1910_2013` LIMIT 10000",
		projection: []interface{}{
			"", "", 0, "", 0,
		},
		repeat: 3,
	},
	{
		description: "structured types",
		SQL:         "SELECT  t.publication_number, t.inventor, t.assignee, t.description_localized FROM `patents-public-data.patents.publications_202105` t ORDER BY 1 LIMIT 1000",
		projection: []interface{}{
			"", []string{}, []string{}, interface{}([]Localized{}),
		},
		repeat: 3,
	},
}

func benchmark(t *testing.B, testGCP bool, testCaseIndex int) {

	projectID := os.Getenv("GCP_PROJECT")
	if projectID == "" {
		t.Skip("set GCP_PROJECT and GOOGLE_APPLICATION_CREDENTIALS before running test")
		return
	}

	testCase := testCases[testCaseIndex]
	projection := make([]interface{}, len(testCase.projection))
	for i := range projection {
		val := reflect.ValueOf(testCase.projection[i])
		ptr := reflect.New(val.Type())
		ptr.Elem().Set(val)
		projection[i] = ptr.Interface()
	}
	if !testGCP {

		elapsed, err := testDatabaseSQLClientQuery(projectID, testCase.SQL, testCase.repeat, projection)
		if !assert.Nil(t, err, testCase.description) {
			return
		}
		fmt.Printf("database/sql: %v %s\n", testCase.description, elapsed)
	}
	if testGCP {
		t.Skip("skipped gcp")
		//elapsed, err := testGCPClientQuery(projectID, testCase.SQL, testCase.repeat, projection)
		//if !assert.Nil(t, err, testCase.description) {
		//	return
		//}
		//fmt.Printf("database/gcp: %v %s\n", testCase.description, elapsed)
	}

}
