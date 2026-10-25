package bigquery

import (
	"context"
	"database/sql/driver"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

type parameterTestTransport struct {
	requests int
}

func (t *parameterTestTransport) RoundTrip(*http.Request) (*http.Response, error) {
	t.requests++
	return nil, errors.New("unexpected BigQuery request")
}

func TestStatement_QueryContextNilParameter(t *testing.T) {
	var key *int
	converted, err := driver.DefaultParameterConverter.ConvertValue(key)
	require.NoError(t, err)
	for _, tt := range []struct {
		name  string
		value interface{}
	}{
		{"untyped", nil},
		{"converted pointer", converted},
		{"unsupported pointer", (**struct{ ID int })(nil)},
		{"unsupported slice", []interface{}(nil)},
		{"unsupported struct pointer", (*struct{ ID int })(nil)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			transport := &parameterTestTransport{}
			service, err := bigquery.NewService(context.Background(), option.WithHTTPClient(&http.Client{Transport: transport}))
			require.NoError(t, err)
			stmt := &Statement{
				projectID: "test-project",
				service:   service,
				job: &bigquery.Job{Configuration: &bigquery.JobConfiguration{
					Query: &bigquery.JobConfigurationQuery{Query: "SELECT ?, ?, ?, ?, ?, ?, ?"},
				}},
			}
			args := make([]driver.NamedValue, 7)
			for i := range args {
				args[i] = driver.NamedValue{Ordinal: i + 1, Value: "private-bound-value"}
			}
			args[6].Value = tt.value
			rows, err := stmt.QueryContext(context.Background(), args)
			require.Nil(t, rows)
			require.ErrorContains(t, err, "failed to convert args to query parameters: query parameter at position 7:")
			require.NotContains(t, err.Error(), "private-bound-value")
			require.Zero(t, transport.requests)
			require.Nil(t, stmt.job.Configuration.Query.QueryParameters)
		})
	}
}

func Test_CheckQueryParameters(t *testing.T) {
	var testCases = []struct {
		description string
		SQL         string
		exepcted    int
	}{
		{
			description: "Merge with inline params",
			SQL: `MERGE INTO
  mock.customers_robin c
USING
  (
  SELECT
    1001 AS id,
    'Sally' AS first_name,
    'Thomas' AS last_name,
    'sally.thomas@acme.com' AS email,
    FALSE AS __artie_delete
  UNION ALL
  SELECT
    1002,
    'George',
    'Bailey',
    'gbailey@foobar.com',
    FALSE
  UNION ALL
  SELECT
    1003,
    'Edward',
    'Walker',
    'ed@walker.com',
    FALSE
  UNION ALL
  SELECT
    1004,
    'Anne',
    'Kretchmar',
    'annek@noanswer.org',
    FALSE) AS cc
ON
  c.id = cc.id
  WHEN MATCHED AND cc.__artie_delete THEN DELETE
  WHEN MATCHED
  AND IFNULL(cc.__artie_delete, FALSE) = FALSE THEN
UPDATE
SET
  id = cc.id,
  first_name = cc.first_name,
  last_name = cc.last_name,
  email = cc.email
  WHEN NOT MATCHED
  AND IFNULL(cc.__artie_delete, FALSE) = FALSE THEN
INSERT
  ( id,
    first_name,
    last_name,
    email )
VALUES
  ( cc.id,cc.first_name,cc.last_name,cc.email );`,
		},

		{
			description: "Merge with binding params",
			SQL: `MERGE INTO
  mock.customers_robin c
USING
  (
  SELECT
    1001 AS id,
    'Sally' AS first_name,
    'Thomas' AS last_name,
    'sally.thomas@acme.com' AS email,
    FALSE AS __artie_delete
  UNION ALL
  SELECT
    1002,
    'George',
    'Bailey',
    'gbailey@foobar.com',
    FALSE
  UNION ALL
  SELECT
    1003,
    'Edward',
    'Walker',
    @email,
    FALSE
  UNION ALL
  SELECT
    1004,
    'Anne',
    'Kretchmar',
    'annek@noanswer.org',
    FALSE) AS cc
ON
  c.id = cc.id
  WHEN MATCHED AND cc.__artie_delete THEN DELETE
  WHEN MATCHED
  AND IFNULL(cc.__artie_delete, FALSE) = FALSE THEN
UPDATE
SET
  id = cc.id,
  first_name = cc.first_name,
  last_name = cc.last_name,
  email = cc.email
  WHEN NOT MATCHED
  AND IFNULL(cc.__artie_delete, FALSE) = FALSE THEN
INSERT
  ( id,
    first_name,
    last_name,
    email )
VALUES
  ( cc.id,cc.first_name,cc.last_name,cc.email );`,
			exepcted: 1,
		},
		{
			description: "legacy decorator",
			SQL:         `SELECT * FROM [project:dataset.table@1700000000000-1700003600000]`,
			exepcted:    0,
		},
	}

	for _, testCase := range testCases {
		actual := checkQueryParameters(testCase.SQL)
		assert.Equal(t, testCase.exepcted, actual, testCase.description)
	}
}
