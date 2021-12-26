package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"time"
)

const (
	statusDone = "DONE"
)

func waitForJobCompletion(ctx context.Context, service *bigquery.Service, projectID string, location, jobReferenceID string) (*bigquery.Job, error) {
	var job *bigquery.Job
	var err error
	waitTime := 30 * time.Millisecond
	for {
		err = runWithRetries(func() error {
			statusCall := service.Jobs.Get(projectID, jobReferenceID)
			statusCall.Location(location)
			job, err = statusCall.Context(ctx).Do()
			return err
		}, 3)
		if err == nil && job.Status.State == statusDone {
			break
		}
		waitTime = waitTime*2 + 1%1000
		time.Sleep(waitTime)
	}
	if job != nil && job.Status != nil && job.Status.ErrorResult != nil {
		errors, _ := json.Marshal(job.Status.Errors)
		return job, fmt.Errorf("%v: %s", job.Status.ErrorResult.Message, errors)
	}
	return job, err
}
