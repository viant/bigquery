package ingestion_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/bigquery"
	"github.com/viant/bigquery/reader"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestService_Ingest(t *testing.T) {

	projectID := os.Getenv("GCP_PROJECT")
	if projectID == "" {
		t.Skip("set GCP_PROJECT and GOOGLE_APPLICATION_CREDENTIALS before running test")
		return
	}
	db, err := sql.Open("bigquery", fmt.Sprintf("bigquery://%v/test", projectID))
	if !assert.Nil(t, err) {
		return
	}

	var testCases = []struct {
		createDDL      string
		description    string
		readerID       string
		table          string
		SQL            string
		hint           string
		loadData       string
		hasError       bool
		expect         int64
		gzipEnabled    bool
		paruqetEnabled bool
	}{
		{
			description:    "PARQUET ingestion 001",
			table:          "ingestion_parquet_case_001",
			createDDL:      "CREATE TABLE ingestion_parquet_case_001 (ID INTEGER, TOTAL FLOAT64, DESC_01 STRING)",
			SQL:            "LOAD 'Reader:parquet:123' DATA INTO TABLE ingestion_parquet_case_001",
			readerID:       "123",
			loadData:       "UEFSMRUGFSQVJBXIl8J1TBUEFQAVBBUAFQQVABIAAAQBAQAAAAAAAAACAAAAAAAAABUGFSQVJBWeu5a1BkwVBBUAFQQVABUEFQASAAAEAWZmZmZmZhxAzczMzMzMHEAVBhUcFRwVuM6i7g9MFQQVABUEFQAVBBUAEgAABAECAAAAQTECAAAAQTIZEgAZGAgBAAAAAAAAABkYCAIAAAAAAAAAFQAZFgAAGRIAGRgIZmZmZmZmHEAZGAjNzMzMzMwcQBUAGRYAABkSABkYAkExGRgCQTIVABkWAAAZHBYIFVoWAAAAGRwWYhVcFgAAABkcFr4BFVQWAAAAFQIZTEgERm9vMhUGABUEFYABFQIYAmlkJSRMrBNAEQAAABUKFYABFQIYBXRvdGFsABUMJQIYB2Rlc2NfMDElAEwcAAAAFgQZHBk8JgAcFQQZJQAGGRgCaWQVABYEFloWWiYISRwVBhUAFQIAABa0AxUUFpICFT4AJgAcFQoZJQAGGRgFdG90YWwVABYEFlwWXCZiSRwVBhUAFQIAABbIAxUUFtACFT4AJgAcFQwZJQAGGRgHZGVzY18wMRUAFgQWVBZUJr4BSRwVBhUAFQIAABbcAxUWFo4DFSYAFooCFgQZDBYIFooCABkMGEFnaXRodWIuY29tL3NlZ21lbnRpby9wYXJxdWV0LWdvIHZlcnNpb24gMC4wLjAoYnVpbGQgNWJkNWY2MTE0NjM4KRk8HAAAHAAAHAAAADkBAABQQVIx",
			hasError:       false,
			expect:         2,
			gzipEnabled:    false,
			paruqetEnabled: true,
		},
		{
			description: "CSV ingestion 001",
			table:       "ingestion_case_001",
			createDDL:   "CREATE TABLE ingestion_case_001 (ID INTEGER, TOTAL FLOAT64, DESC_01 STRING, DESC_02 STRING, DESC_03 STRING, DESC_04 STRING)",
			SQL:         "LOAD 'Reader:csv:123' DATA INTO TABLE ingestion_case_001",
			readerID:    "123",
			loadData: `1,,,"B,"",1","null","C1"
2,,,"B,""2","null","C2 ` + time.Now().String() + `"`,
			hasError:    false,
			expect:      2,
			gzipEnabled: false,
		},
		{
			description: "JSON ingestion 001",
			table:       "ingestion_case_001",
			createDDL:   "CREATE TABLE ingestion_case_001 (ID INTEGER, TOTAL FLOAT64, DESC_01 STRING, DESC_02 STRING, DESC_03 STRING, DESC_04 STRING)",
			SQL:         "LOAD 'Reader:json:123' DATA INTO TABLE ingestion_case_001",
			readerID:    "123",
			loadData: `{"ID":1,"Total":0,"Desc_01":"A","Desc_02":"B","Desc_03":"C","Desc_04":"D"}
{"ID":2,"Total":0,"Desc_01":"A","Desc_02":"B","Desc_03":"C","Desc_04":"D"}`,
			hasError:    false,
			expect:      2,
			gzipEnabled: false,
		},
		{
			description:    "PARQUET ingestion with gzip compression 001",
			table:          "ingestion_parquet_case_001",
			createDDL:      "CREATE TABLE ingestion_parquet_case_001 (ID INTEGER, TOTAL FLOAT64, DESC_01 STRING)",
			SQL:            "LOAD 'Reader:parquet:123' DATA INTO TABLE ingestion_parquet_case_001",
			readerID:       "123",
			loadData:       "UEFSMRUGFSQVJBXIl8J1TBUEFQAVBBUAFQQVABIAAAQBAQAAAAAAAAACAAAAAAAAABUGFSQVJBWeu5a1BkwVBBUAFQQVABUEFQASAAAEAWZmZmZmZhxAzczMzMzMHEAVBhUcFRwVuM6i7g9MFQQVABUEFQAVBBUAEgAABAECAAAAQTECAAAAQTIZEgAZGAgBAAAAAAAAABkYCAIAAAAAAAAAFQAZFgAAGRIAGRgIZmZmZmZmHEAZGAjNzMzMzMwcQBUAGRYAABkSABkYAkExGRgCQTIVABkWAAAZHBYIFVoWAAAAGRwWYhVcFgAAABkcFr4BFVQWAAAAFQIZTEgERm9vMhUGABUEFYABFQIYAmlkJSRMrBNAEQAAABUKFYABFQIYBXRvdGFsABUMJQIYB2Rlc2NfMDElAEwcAAAAFgQZHBk8JgAcFQQZJQAGGRgCaWQVABYEFloWWiYISRwVBhUAFQIAABa0AxUUFpICFT4AJgAcFQoZJQAGGRgFdG90YWwVABYEFlwWXCZiSRwVBhUAFQIAABbIAxUUFtACFT4AJgAcFQwZJQAGGRgHZGVzY18wMRUAFgQWVBZUJr4BSRwVBhUAFQIAABbcAxUWFo4DFSYAFooCFgQZDBYIFooCABkMGEFnaXRodWIuY29tL3NlZ21lbnRpby9wYXJxdWV0LWdvIHZlcnNpb24gMC4wLjAoYnVpbGQgNWJkNWY2MTE0NjM4KRk8HAAAHAAAHAAAADkBAABQQVIx",
			hasError:       true,
			expect:         2,
			gzipEnabled:    true,
			paruqetEnabled: true,
		},
		{
			description: "CSV ingestion with gzip compression 001",
			table:       "ingestion_case_001",
			createDDL:   "CREATE TABLE ingestion_case_001 (ID INTEGER, TOTAL FLOAT64, DESC_01 STRING, DESC_02 STRING, DESC_03 STRING, DESC_04 STRING)",
			SQL:         "LOAD 'Reader:csv:123' DATA INTO TABLE ingestion_case_001",
			readerID:    "123",
			loadData: `1,,,"B,"",1","null","C1"
2,,,"B,""2","null","C2 ` + time.Now().String() + `"`,
			hasError:    false,
			expect:      2,
			gzipEnabled: true,
		},
		{
			description: "JSON ingestion with gzip compression 001",
			table:       "ingestion_case_001",
			createDDL:   "CREATE TABLE ingestion_case_001 (ID INTEGER, TOTAL FLOAT64, DESC_01 STRING, DESC_02 STRING, DESC_03 STRING, DESC_04 STRING)",
			SQL:         "LOAD 'Reader:json:123' DATA INTO TABLE ingestion_case_001",
			readerID:    "123",
			loadData: `{"ID":1,"Total":0,"Desc_01":"A","Desc_02":"B","Desc_03":"C","Desc_04":"D"}
{"ID":2,"Total":0,"Desc_01":"A","Desc_02":"B","Desc_03":"C","Desc_04":"D"}`,
			hasError:    false,
			expect:      2,
			gzipEnabled: true,
		},
		{
			description: "CSV ingestion 004 custom settings with jagged rows",
			table:       "ingestion_case_001",
			createDDL:   "CREATE TABLE ingestion_case_001 (ID INTEGER, TOTAL FLOAT64, DESC_01 STRING, DESC_02 STRING, DESC_03 STRING, DESC_04 STRING)",
			SQL:         "LOAD 'Reader:csv:123' /*+ HINT_WILL_BE_REPLACED_HERE +*/ DATA INTO TABLE ingestion_case_001",
			hint: `{
    "allowJaggedRows": true,
    "allowQuotedNewlines": true,
    "createDisposition": "CREATE_NEVER",
    "destinationTable": {
        "datasetId": "BAZA_02_US",
        "projectId": "snappy-analog-357718",
        "tableId": "invoice"
    },
    "fieldDelimiter": "\u001f",
    "maxBadRecords": 2,
    "quote": "\"",
    "sourceFormat": "CSV",
    "writeDisposition": "WRITE_APPEND"
}`,
			readerID: "123",
			loadData: `1"B
,""1""null""C
text under C1"
2"B
,""2""null""C
text under C2"
3"B
,""3""null"
4"B
,""4""null""C
text under C4"
5"B
,""5""null"
6"B
,""6""null""C
text under C6"`,
			hasError:    false,
			expect:      6,
			gzipEnabled: false,
		},
		{
			description: "CSV ingestion 003 custom settings",
			table:       "ingestion_case_001",
			createDDL:   "CREATE TABLE ingestion_case_001 (ID INTEGER, TOTAL FLOAT64, DESC_01 STRING, DESC_02 STRING, DESC_03 STRING, DESC_04 STRING)",
			SQL:         "LOAD 'Reader:csv:123' /*+ HINT_WILL_BE_REPLACED_HERE +*/ DATA INTO TABLE ingestion_case_001",
			hint: `{
    "allowJaggedRows": true,
    "allowQuotedNewlines": true,
    "createDisposition": "CREATE_NEVER",
    "destinationTable": {
        "datasetId": "BAZA_02_US",
        "projectId": "snappy-analog-357718",
        "tableId": "invoice"
    },
    "fieldDelimiter": "\u001f",
    "maxBadRecords": 2,
    "quote": "\"",
    "sourceFormat": "CSV",
    "writeDisposition": "WRITE_APPEND"
}`,
			readerID: "123",
			loadData: `1"B,""1""null""C1"
2"B,""2""null""C2 ` + time.Now().String() + `"`,
			hasError:    false,
			expect:      2,
			gzipEnabled: false,
		},
		{
			description: "CSV ingestion 002 custom settings",
			table:       "ingestion_case_001",
			createDDL:   "CREATE TABLE ingestion_case_001 (ID INTEGER, TOTAL FLOAT64, DESC_01 STRING, DESC_02 STRING, DESC_03 STRING, DESC_04 STRING)",
			SQL:         "LOAD 'Reader:csv:123' /*+ HINT_WILL_BE_REPLACED_HERE +*/ DATA INTO TABLE ingestion_case_001",
			hint: `{
    "allowQuotedNewlines": true,
    "createDisposition": "CREATE_NEVER",
    "destinationTable": {
        "datasetId": "BAZA_02_US",
        "projectId": "snappy-analog-357718",
        "tableId": "invoice"
    },
    "fieldDelimiter": ",",
    "maxBadRecords": 2,
    "quote": "\"",
    "sourceFormat": "CSV",
    "writeDisposition": "WRITE_APPEND"
}`,
			readerID: "123",
			loadData: `1,,,"B,"",1","null","C1"
2,,,"B,""2","null","C2 ` + time.Now().String() + `"`,
			hasError: false,
			expect:   2,
		},
		{
			description: "CSV ingestion 001",
			table:       "ingestion_case_001",
			createDDL:   "CREATE TABLE ingestion_case_001 (ID INTEGER, TOTAL FLOAT64, DESC_01 STRING, DESC_02 STRING, DESC_03 STRING, DESC_04 STRING)",
			SQL:         "LOAD 'Reader:csv:123' DATA INTO TABLE ingestion_case_001",
			readerID:    "123",
			loadData: `1,,,"B,"",1","null","C1"
2,,,"B,""2","null","C2 ` + time.Now().String() + `"`,
			hasError: false,
			expect:   2,
		},
	}

	for _, testCase := range testCases {
		func() {

			_, err := db.Exec("DROP TABLE IF EXISTS " + testCase.table)
			if !assert.Nil(t, err, testCase.description) {
				return
			}

			_, err = db.Exec(testCase.createDDL)
			if !assert.Nil(t, err, testCase.description) {
				return
			}

			var dataReader io.Reader

			if !testCase.paruqetEnabled {
				dataReader = strings.NewReader(testCase.loadData)
			} else {
				dataPq, err := base64.StdEncoding.DecodeString(testCase.loadData)
				if !assert.Nil(t, err, testCase.description) {
					return
				}
				dataReader = bytes.NewReader(dataPq)
			}

			if testCase.gzipEnabled {
				dataReader, err = reader.Gzip(dataReader)

				if !assert.Nil(t, err, testCase.description) {
					return
				}
			}
			err = reader.Register(testCase.readerID, dataReader)
			defer reader.Unregister(testCase.readerID)
			if !assert.Nil(t, err, testCase.description) {
				return
			}

			testCase.SQL = strings.Replace(testCase.SQL, "HINT_WILL_BE_REPLACED_HERE", testCase.hint, 1)

			ctx := context.Background()
			affected, err := db.ExecContext(ctx, testCase.SQL)
			if testCase.hasError {
				assert.NotNil(t, err, testCase.description)
				return
			}
			if !assert.Nil(t, err, testCase.description) {
				return
			}

			actual, err := affected.RowsAffected()
			if !assert.Nil(t, err, testCase.description) {
				return
			}

			assert.EqualValues(t, testCase.expect, actual)

		}()
	}
}
