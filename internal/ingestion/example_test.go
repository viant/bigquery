package ingestion_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/bigquery/reader"
	"log"
	"strings"
)

func ExampleNewService() {
	projectID := "my-gcp-project"
	db, err := sql.Open("bigquery", fmt.Sprintf("bigquery://%v/test", projectID))
	if err != nil {
		log.Fatal(err)
	}
	readerID := "123456"
	csvReader := strings.NewReader("1,Name 1,Test")
	err = reader.Register(readerID, csvReader)
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Unregister(readerID)
	SQL := fmt.Sprintf(`LOAD 'Reader:csv:%v' DATA INTO TABLE mytable`, readerID)
	result, err := db.ExecContext(context.TODO(), SQL)
	if err != nil {
		log.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	fmt.Printf("loaded: %v rows", affected)
}
