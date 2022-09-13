package ingestion

import (
	"encoding/json"
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

type configLoad bigquery.JobConfigurationLoad

func (s *Service) prepareLoadConfig(ingestion *ingestion) (*bigquery.JobConfigurationLoad, error) {
	config := configLoad{}
	if aHint := ingestion.Hint; aHint != "" {
		if err := json.Unmarshal([]byte(aHint), &config); err != nil {
			return nil, err
		}
	}
	config.DestinationTable = &bigquery.TableReference{
		DatasetId: ingestion.Destination.DatasetID,
		ProjectId: ingestion.Destination.ProjectID,
		TableId:   ingestion.Destination.TableID,
	}

	switch strings.ToUpper(ingestion.Format) {
	case "CSV":
		config.initCSV()
	case "JSON":
		config.initJSON()
	case "PARQUET":
		config.initPARQUET()
	default:
		return nil, fmt.Errorf("unsupported load formt: %v", ingestion.Format)
	}
	result := bigquery.JobConfigurationLoad(config)
	return &result, nil
}

func (c *configLoad) initCSV() {
	if c.SourceFormat == "" {
		c.SourceFormat = "CSV"
	}
}

func (c *configLoad) initJSON() {
	if c.SourceFormat == "" {
		c.SourceFormat = "NEWLINE_DELIMITED_JSON"
	}
}

func (c *configLoad) initPARQUET() {
	if c.SourceFormat == "" {
		c.SourceFormat = "PARQUET"
	}
}
