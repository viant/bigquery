package bigquery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestRows_Next(t *testing.T) {

	projectID := os.Getenv("GCP_PROJECT")
	if projectID == "" {
		t.Skip("set GCP_PROJECT and GOOGLE_APPLICATION_CREDENTIALS before running test")
		return
	}
	db, err := sql.Open("bigquery", fmt.Sprintf("bigquery://%v/test", projectID))
	if err != nil {
		t.Logf("failed to connect to %v, %v", projectID, err)
		return
	}

	var testCases = []struct {
		description string
		SQL         string
		expect      [][]interface{}
		params      []interface{}
	}{
		{
			description: "query primitive",
			SQL: `SELECT 1 AS f1, "test 1" AS f2, 3.4 AS f3, TIMESTAMP("2020-01-01 00:00:00 UTC") AS f4 UNION ALL
				  SELECT 2 AS f1, "test 2" AS f2, 2.4 AS f3, TIMESTAMP("2020-01-02 00:00:00 UTC") AS f4`,
			expect: [][]interface{}{
				{
					1, "test 1", 3.4, asTimestamp("2020-01-01T00:00:00"),
				},
				{
					2, "test 2", 2.4, asTimestamp("2020-01-02T00:00:00"),
				},
			},
		},
		{
			description: "nested/repeated",
			SQL: `WITH races AS (
		  SELECT "800M" AS race,
		    [STRUCT("Ben" as name, [23.6, 26.3] as splits), 
		 	 STRUCT("Frank" as name, [23.4, 26.3] as splits)
			]
		       AS participants)
		SELECT
		  race,
		  participant
		FROM races r
		CROSS JOIN UNNEST(r.participants) as participant`,
			expect: [][]interface{}{
				{
					"800M", []byte(`{"name":"Ben","splits":[23.6,26.3]}`),
				},
				{
					"800M", []byte(`{"name":"Frank","splits":[23.4,26.3]}`),
				},
			},
		},
		{
			description: "named parametrized query",
			SQL: `SELECT word, word_count
        FROM ` + "`bigquery-public-data.samples.shakespeare`" + `
        WHERE corpus = @corpus
        AND word_count >= @min_word_count
        ORDER BY word_count DESC LIMIT 2`,
			params: []interface{}{
				sql.Named("corpus", "romeoandjuliet"),
				sql.Named("min_word_count", 250),
			},
			expect: [][]interface{}{
				{
					"the", 614,
				},
				{
					"I", 577,
				},
			},
		},

		{
			description: "parametrized query",
			SQL: `SELECT word, word_count
        FROM ` + "`bigquery-public-data.samples.shakespeare`" + `
        WHERE corpus = ?
        AND word_count >= ?
        ORDER BY word_count DESC LIMIT 2`,
			params: []interface{}{"romeoandjuliet", 250},
			expect: [][]interface{}{
				{
					"the", 614,
				},
				{
					"I", 577,
				},
			},
		},
		{
			description: "hints with legacy mode",
			SQL:         "SELECT  /*+ {\"UseLegacySql\": true} +*/ HASH(12)",
			expect: [][]interface{}{
				{
					-3369419977847865783,
				},
			},
		},
		{
			description: "ExpandJobID hint ",
			SQL:         "SELECT  /*+ {\"ExpandDSN\": true} +*/ '$ProjectID' AS PROJECT_ID",
			expect: [][]interface{}{
				{
					os.Getenv("GCP_PROJECT"),
				},
			},
		},
	}

	for _, testCase := range testCases {
		stmt, err := db.PrepareContext(context.Background(), testCase.SQL)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		rows, err := stmt.QueryContext(context.Background(), testCase.params...)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		types, _ := rows.ColumnTypes()
		var actual = make([]interface{}, len(types))
		var binding = make([]interface{}, len(types))
		for i := range binding {
			binding[i] = &actual[i]
		}
		k := 0
		for rows.Next() {
			err := rows.Err()
			if !assert.Nil(t, err, testCase.description) {
				break
			}
			err = rows.Scan(binding...)
			if !assert.Nil(t, err, testCase.description) {
				continue
			}
			for i := range testCase.expect {
				expect := testCase.expect[k][i]
				if e, ok := expect.([]byte); ok {
					expectValue := reflect.New(types[i].ScanType())
					expectPtr := expectValue.Interface()
					err = json.Unmarshal(e, expectPtr)
					if !assert.Nil(t, err, testCase.description) {
						continue
					}
					expect = expectValue.Elem().Interface()
				}
				assert.EqualValues(t, expect, actual[i], testCase.description)
			}
			k++
		}
		assert.EqualValues(t, len(testCase.expect), k, testCase.description)
		_ = stmt.Close()
		_ = rows.Close()
	}

}

func asTimestamp(t string) time.Time {
	ts, _ := time.ParseInLocation(time.RFC3339, t, time.UTC)
	return ts
}
