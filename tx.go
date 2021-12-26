package bigquery

type tx struct {
	*connection
}

func (t *tx) Commit() error {
	return nil
}

func (t *tx) Rollback() error {
	return nil
}
