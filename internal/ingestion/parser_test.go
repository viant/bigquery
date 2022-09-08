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
			SQL:         "LOAD 'Reader:csv:123e4567-e89b-12d3-a456-426614174012' DATA INTO TABLE project.set.table",
			hasError:    false,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "LOAD",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with invalid destination",
			SQL:         "LOAD 'Reader:csv:123e4567-e89b-12d3-a456-426614174012' DATA INTO TABLE project.9set.table",
			hasError:    true,
		},
		{
			description: "CSV load with absolute destination - all upper case",
			SQL:         "LOAD 'READER:CSV:123E4567-E89B-12D3-A456-426614174012' DATA INTO TABLE PROJECT.SET.TABLE",
			hasError:    false,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "PROJECT",
					DatasetID: "SET",
					TableID:   "TABLE",
				},
				Kind:     "LOAD",
				Format:   "CSV",
				ReaderID: "123E4567-E89B-12D3-A456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case",
			SQL:         "load 'reader:csv:123e4567-e89b-12d3-a456-426614174012' data into table project.set.table",
			hasError:    false,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		////
		// lowercase
		////
		{
			description: "CSV load with absolute destination - all lower case - additional whitespace not allowed 01",
			SQL:         " load 'reader:csv:123e4567-e89b-12d3-a456-426614174012' data into table project.set.table",
			hasError:    true,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespace not allowed 02",
			SQL:         "load ' reader:csv:123e4567-e89b-12d3-a456-426614174012' data into table project.set.table",
			hasError:    true,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespace not allowed 103",
			SQL:         "load 'reader :csv:123e4567-e89b-12d3-a456-426614174012' data into table project.set.table",
			hasError:    true,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespace not allowed 04",
			SQL:         "load 'reader: csv:123e4567-e89b-12d3-a456-426614174012' data into table project.set.table",
			hasError:    true,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespace not allowed 05",
			SQL:         "load 'reader:csv :123e4567-e89b-12d3-a456-426614174012' data into table project.set.table",
			hasError:    true,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespace allowed 106",
			SQL:         "load 'reader:csv: 123e4567-e89b-12d3-a456-426614174012' data into table project.set.table",
			hasError:    false,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: " 123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespace allowed 107",
			SQL:         "load 'reader:csv:123e4567-e89b-12d3-a456-426614174012 ' data into table project.set.table",
			hasError:    false,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012 ",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespace not allowed 08",
			SQL:         "load 'reader:csv:123e4567-e89b-12d3-a456-426614174012' data into table project .set.table",
			hasError:    true,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespace not allowed 09",
			SQL:         "load 'reader:csv:123e4567-e89b-12d3-a456-426614174012' data into table project. set.table",
			hasError:    true,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespace not allowed 10",
			SQL:         "load 'reader:csv:123e4567-e89b-12d3-a456-426614174012' data into table project.set .table",
			hasError:    true,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespace not allowed 11",
			SQL:         "load 'reader:csv:123e4567-e89b-12d3-a456-426614174012' data into table project.set. table",
			hasError:    true,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespaces allowed 01",
			SQL:         "load  'reader:csv:123e4567-e89b-12d3-a456-426614174012' data into table project.set.table",
			hasError:    false,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespaces allowed 02",
			SQL:         "load 'reader:csv:123e4567-e89b-12d3-a456-426614174012'  data into table project.set.table",
			hasError:    false,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespaces allowed 03",
			SQL:         "load 'reader:csv:123e4567-e89b-12d3-a456-426614174012' data  into table project.set.table",
			hasError:    false,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespaces allowed 04",
			SQL:         "load 'reader:csv:123e4567-e89b-12d3-a456-426614174012' data into  table project.set.table",
			hasError:    false,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespaces allowed 05",
			SQL:         "load 'reader:csv:123e4567-e89b-12d3-a456-426614174012' data into table  project.set.table",
			hasError:    false,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - additional whitespace not allowed 06",
			SQL:         "load 'reader:csv:123e4567-e89b-12d3-a456-426614174012' data into table project.set.table ",
			hasError:    false,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - missing whitespace 01",
			SQL:         "load'reader:csv:123e4567-e89b-12d3-a456-426614174012' data into table project.set.table",
			hasError:    true,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - missing whitespace 02",
			SQL:         "load 'reader:csv:123e4567-e89b-12d3-a456-426614174012'data into table project.set.table",
			hasError:    true,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - missing whitespace 03",
			SQL:         "load 'reader:csv:123e4567-e89b-12d3-a456-426614174012' datainto table project.set.table",
			hasError:    true,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - missing whitespace 04",
			SQL:         "load 'reader:csv:123e4567-e89b-12d3-a456-426614174012' data intotable project.set.table",
			hasError:    true,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
			},
		},
		{
			description: "CSV load with absolute destination - all lower case - missing whitespace 05",
			SQL:         "load 'reader:csv:123e4567-e89b-12d3-a456-426614174012' data into tableproject.set.table",
			hasError:    true,
			expect: &Ingestion{
				Destination: &Destination{
					ProjectID: "project",
					DatasetID: "set",
					TableID:   "table",
				},
				Kind:     "load",
				Format:   "csv",
				ReaderID: "123e4567-e89b-12d3-a456-426614174012",
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
