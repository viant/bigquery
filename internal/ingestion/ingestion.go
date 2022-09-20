package ingestion

type kind string

const (
	//KindLoad means supported load data ability
	KindLoad = kind("LOAD")

	// KindStream means supported stream data ability
	KindStream = kind("STREAM")
)

type (
	destination struct {
		ProjectID string
		DatasetID string
		TableID   string
	}

	ingestion struct {
		Destination   *destination
		Kind          kind
		Format        string
		InsertIDField string
		ReaderID      string
		Hint          string
	}
)

func (d *destination) init(projectID string, datasetID string) {
	if d.ProjectID == "" {
		d.ProjectID = projectID
	}
	if d.DatasetID == "" {
		d.DatasetID = datasetID
	}
}
