package ingestion

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/bigquery/internal/exec"
	"github.com/viant/bigquery/internal/hint"
	"github.com/viant/bigquery/reader"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"io"
	"strings"
)

const (
	// Maximum rows per request allowed are:
	// 10000 (at 2020 year)
	// 50000 (at 2022 year)
	// but a maximum of 500 rows per request is recommended (at 2020 and 2022 year)
	//https://cloud.google.com/bigquery/quotas#streaming_inserts
	maxStreamBatchCount = int64(9999)
	attempts            = 3
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
	case KindStream:
		return s.stream(ctx, aIngestion)
	default:
		return 0, fmt.Errorf("unsupported kind: %s, supported: [%s|%s]", aIngestion.Kind, KindLoad, KindStream)
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
func (s *Service) stream(ctx context.Context, ingestion *ingestion) (int64, error) {

	aReader, err := reader.Get(ingestion.ReaderID)
	if err != nil {
		return 0, err
	}

	rows := make([]*bigquery.TableDataInsertAllRequestRows, 0)

	err2 := s.readRows(aReader, ingestion, &rows)
	if err2 != nil {
		return 0, err2
	}

	return s.streamAll(ctx, rows, ingestion.Destination.TableID)
}

func (s *Service) readRows(aReader io.Reader, ingestion *ingestion, rows *[]*bigquery.TableDataInsertAllRequestRows) error {
	var buffer = new(bytes.Buffer)
	lineReader := bufio.NewReader(aReader)
	for i := 0; ; i++ {
		buffer.Reset()
	readLine:
		line, isPrefix, err := lineReader.ReadLine()
		buffer.Write(line)
		if err != nil {
			break
		}
		if isPrefix {
			goto readLine
		}
		rawLine := buffer.Bytes()
		if len(rawLine) == 0 {
			continue
		}
		row := bigquery.TableDataInsertAllRequestRows{}
		if err := json.Unmarshal(buffer.Bytes(), &row.Json); err != nil {
			return err
		}
		if key := ingestion.InsertIDField; key != "" {
			if row.InsertId, err = extractJSONKeyValue(buffer.Bytes(), key); err != nil {
				return err
			}
		}
		*rows = append(*rows, &row)
	}
	return nil
}

func (s *Service) streamAll(ctx context.Context, allRows []*bigquery.TableDataInsertAllRequestRows, tableID string) (int64, error) {

	allRowsCount := int64(len(allRows))
	offset := int64(0)

	for offset < allRowsCount {
		cnt := min(allRowsCount-offset, maxStreamBatchCount)
		rows := allRows[offset : offset+cnt]
		offset += cnt

		insertCall, err := s.streamRows(ctx, rows, tableID)
		if err == nil {
			err = toInsertError(insertCall.InsertErrors)
		}
		if err != nil {
			return 0, err
		}
	}

	return offset, nil
}

func (s *Service) streamRows(ctx context.Context, rows []*bigquery.TableDataInsertAllRequestRows, tableID string) (*bigquery.TableDataInsertAllResponse, error) {

	var response *bigquery.TableDataInsertAllResponse
	var err error

	err = exec.RunWithRetries(func() error {
		insertRequest := &bigquery.TableDataInsertAllRequest{}
		insertRequest.Rows = rows

		requestCall := s.service.Tabledata.InsertAll(s.projectID, s.datasetID, tableID, insertRequest)
		response, err = requestCall.Context(ctx).Do()
		return err
	}, attempts)

	return response, err
}

func toInsertError(insertErrors []*bigquery.TableDataInsertAllResponseInsertErrors) error {
	if insertErrors == nil {
		return nil
	}
	var messages = make([]string, 0)
	for _, insertError := range insertErrors {
		if len(insertError.Errors) > 0 {
			info := insertError.Errors[0].Message
			messages = append(messages, info)
			break
		}
	}
	if len(messages) > 0 {
		return fmt.Errorf("%s", strings.Join(messages, ","))
	}
	return fmt.Errorf("%v", insertErrors[0])
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

func min(x, y int64) int64 {
	if x > y {
		return y
	}
	return x
}
