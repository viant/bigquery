package bigquery

import "google.golang.org/api/bigquery/v2"

const (
	dsnProjectID = "$ProjectID"
	dsnDatasetID = "$DatasetID"
	dsnLocation  = "$Location"
)

//queryHint represents query hint struct
type queryHint struct {
	bigquery.JobConfigurationQuery
	ExpandDSN bool //Expand the following variables $ProjectID, $DatasetID, $Location
}
