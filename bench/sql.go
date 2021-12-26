package bench

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"
)

func testDatabaseSQLClientQuery(projectID, SQL string, repeat int, values []interface{}) (time.Duration, error) {
	started := time.Now()
	db, err := sql.Open("bigquery", fmt.Sprintf("bigquery://%v/test", projectID))
	if err != nil {
		return 0, err
	}
	defer db.Close()
	stmt, err := db.PrepareContext(context.TODO(), SQL)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	for i := 0; i < repeat; i++ {
		rows, err := stmt.Query()
		if err != nil {
			return 0, err
		}

		types, err := rows.ColumnTypes()
		if err != nil {
			return 0, err
		}
		for i, v := range values { //runs only once per test
			if reflect.TypeOf(v) !=  types[i].ScanType() {
				values[i] = reflect.New(types[i].ScanType()).Interface()
			}
		}
		for rows.Next() {

			if rows.Err() != nil {
				return 0, rows.Err()
			}
			err = rows.Scan(values...)
			if err != nil {
				return 0, err
			}
		}
		_ = rows.Close()
	}
	return time.Now().Sub(started), nil
}

