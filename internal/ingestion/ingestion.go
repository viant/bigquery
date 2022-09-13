package ingestion

type Kind string

const (
	KindLoad   = Kind("LOAD")
	KindStream = Kind("STREAM")
)

type (
	Destination struct {
		ProjectID string
		DatasetID string
		TableID   string
	}

	Ingestion struct {
		Destination *Destination
		Kind        Kind
		Format      string
		ReaderID    string
	}
)
