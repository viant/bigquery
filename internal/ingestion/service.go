package ingestion

import (
	"bytes"
	"context"
	"fmt"
	"github.com/viant/bigquery/internal/exec"
	"github.com/viant/bigquery/internal/hint"
	"github.com/viant/bigquery/reader"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"io"
	"strings"
)

// Service represents ingestion service
type Service struct {
	service   *bigquery.Service
	projectID string
	datasetID string
	location  string
}

// NewService creates Service
func NewService(service *bigquery.Service, projectID, datasetID, location string) *Service {
	return &Service{
		service:   service,
		projectID: projectID,
		datasetID: datasetID,
		location:  location,
	}
}

// Ingest ingests data into a database
func (s *Service) Ingest(ctx context.Context, SQL string) (int64, error) {
	aHint := hint.Extract(SQL)

	SQL = s.deleteHint(SQL)

	aIngestion, err := parse(SQL)
	if err != nil {
		return 0, err
	}
	aIngestion.Hint = aHint
	aIngestion.Destination.init(s.projectID, s.datasetID)

	switch aIngestion.Kind {
	case KindLoad:
		return s.load(ctx, aIngestion)
	default:
		return 0, fmt.Errorf("unsupported kind: %s, supported: [%s]", aIngestion.Kind, KindLoad)
	}
}

// load loads data into BigQuery
func (s *Service) load(ctx context.Context, ingestion *ingestion) (int64, error) {

	aConfigLoad, err := s.prepareLoadConfig(ingestion)
	if err != nil {
		return 0, err
	}

	isReaderRequired := len(aConfigLoad.SourceUris) == 0

	var aReader io.Reader
	if isReaderRequired {
		if aReader, err = reader.Get(ingestion.ReaderID); err != nil {
			return 0, err
		}
	}

	job := s.createJob(aConfigLoad)

	job, err = s.submitJob(ctx, job, aReader)
	if err != nil {
		return 0, err
	}

	job, err = exec.WaitForJobCompletion(ctx, s.service, s.projectID, s.location, job.JobReference.JobId)
	if err != nil {
		return 0, err
	}

	statistics := job.Statistics.Load
	var affected int64 = 0

	if statistics != nil {
		affected = statistics.OutputRows
	}

	return affected, nil
}

// stream streams data into BigQuery
func (s *Service) stream(ctx context.Context, ingestion *ingestion) (int, error) {
	return 0, fmt.Errorf("Unsupported function stream")
}

// submitJob submits job, returns affected rows count and error
func (s *Service) submitJob(ctx context.Context, job *bigquery.Job, reader io.Reader) (*bigquery.Job, error) {
	bigqueryService := s.service

	if reader != nil {
		return s.submitJobWithReader(ctx, job, reader, bigqueryService)
	}
	err := exec.RunWithRetries(func() error {
		var err error
		call := bigqueryService.Jobs.Insert(s.projectID, job)
		job, err = call.Context(ctx).Do()
		return err
	}, 3)
	return job, err
}

func (s *Service) submitJobWithReader(ctx context.Context, job *bigquery.Job, reader io.Reader, bigqueryService *bigquery.Service) (*bigquery.Job, error) {
	buf, err := s.prepareBufferedReader(reader)
	if err != nil {
		return nil, err
	}
	err = exec.RunWithRetries(func() error {
		call := bigqueryService.Jobs.Insert(s.projectID, job)

		//call = call.Media(bytes.NewBuffer(buf.Bytes()), googleapi.ContentType("application/x-gzip"))
		call = call.Media(bytes.NewBuffer(buf.Bytes()), googleapi.ContentType("application/octet-stream"))
		//call.Header().Set("Content-Encoding", "gzip")
		job, err = call.Context(ctx).Do()
		return err
	}, 3)
	return job, err
}

func (s *Service) prepareBufferedReader(reader io.Reader) (*bytes.Buffer, error) {
	if bufReader, ok := reader.(*bytes.Buffer); ok {
		return bufReader, nil
	}
	buf := new(bytes.Buffer)
	_, err := io.Copy(buf, reader)
	if err != nil {
		return nil, err
	}
	return buf, err

}

// createJob creates job
func (s *Service) createJob(loadConfig *bigquery.JobConfigurationLoad) *bigquery.Job {

	return &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Load: loadConfig,
		},
	}
}

func (s *Service) deleteHint(SQL string) string {
	start := strings.Index(SQL, "/*+")
	end := strings.Index(SQL, "+*/")
	if start != -1 && end != -1 {
		return SQL[:start] + SQL[end+1+2:]
	}
	return SQL
}
