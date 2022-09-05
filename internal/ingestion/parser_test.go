package ingestion

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"testing"
)

func TestParse(t *testing.T) {

	var testCases = []struct {
		description string
		SQL         string
		hasError    bool
		expect      *Ingestion
	}{
		{
			description: "CSV load with absolute destination",
			SQL:         "LOAD 'Reader:csv:<uid>' DATA INTO TABLE project.set.table",
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "LOAD",
				Format:   "CSV",
				ReaderID: "<uid>",
			},
		},
	}

	for _, testCase := range testCases {
		actual, err := Parse(testCase.SQL)
		if testCase.hasError {
			assert.NotNil(t, err, testCase.description)
			continue
		}
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		if !assertly.AssertValues(t, testCase.expect, actual, testCase.description) {
			data, _ := json.Marshal(actual)
			fmt.Printf("%s\n", data)
		}
	}

}
