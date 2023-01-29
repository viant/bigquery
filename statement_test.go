package bigquery

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

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
	}

	for _, testCase := range testCases {
		actual := checkQueryParameters(testCase.SQL)
		assert.Equal(t, testCase.exepcted, actual, testCase.description)
	}
}
