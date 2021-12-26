package bigquery

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

//ExampleRowsNext fetching rows example
func ExampleRowsNext() {

	type Participant struct {
		Name   string
		Splits []float64
	}

	projectID := ""
	db, err := sql.Open("bigquery", fmt.Sprintf("bigquery://%v/test", projectID))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	SQL := `WITH races AS (
		  SELECT "800M" AS race,
		    [STRUCT("Ben" as name, [23.4, 26.3] as splits), 
		 	 STRUCT("Frank" as name, [23.4, 26.3] as splits)
			]
		       AS participants)
		SELECT
		  race,
		  participant
		FROM races r
		CROSS JOIN UNNEST(r.participants) as participant`

	rows, err := db.QueryContext(context.Background(), SQL)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
		var race string
		var participant Participant
		err = rows.Scan(&race, &participant)
		fmt.Printf("fetched: %v %+v\n", race, participant)
		if err != nil {
			log.Fatal(err)
		}
	}
}
