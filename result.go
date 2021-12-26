package bigquery

import (
	"errors"
)

var errLastInsertID = errors.New("lastInsertId is not supported")

type result struct {
	totalRows int64
}

//LastInsertId returns not supported error
func (r *result) LastInsertId() (int64, error) {
	return 0, errLastInsertID
}

//RowsAffected return affected rows
func (r *result) RowsAffected() (int64, error) {
	return r.totalRows, nil
}
