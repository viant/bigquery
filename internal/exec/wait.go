package exec

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"time"
)

const (
	//StatusDone status done
	StatusDone = "DONE"
)

// WaitForJobCompletion waits for job completion
func WaitForJobCompletion(ctx context.Context, service *bigquery.Service, projectID string, location, jobReferenceID string) (*bigquery.Job, error) {
	var job *bigquery.Job
	var err error
	waitTime := 30 * time.Millisecond
	maxWaitTime := 2 * time.Second
	for {
		err = RunWithRetries(func() error {
			statusCall := service.Jobs.Get(projectID, jobReferenceID)
			statusCall.Location(location)
			job, err = statusCall.Context(ctx).Do()
			return err
		}, 3)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				cancelJob(service, projectID, location, jobReferenceID)
				return job, ctxErr
			}
			return job, err
		}
		if job != nil && job.Status != nil && job.Status.State == StatusDone {
			break
		}
		select {
		case <-ctx.Done():
			cancelJob(service, projectID, location, jobReferenceID)
			return job, ctx.Err()
		case <-time.After(waitTime):
		}
		waitTime *= 2
		if waitTime > maxWaitTime {
			waitTime = maxWaitTime
		}
	}
	if job != nil && job.Status != nil && job.Status.ErrorResult != nil {
		errors, _ := json.Marshal(job.Status.Errors)
		return job, fmt.Errorf("%v: %s", job.Status.ErrorResult.Message, errors)
	}
	return job, err
}

func cancelJob(service *bigquery.Service, projectID string, location, jobReferenceID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cancelCall := service.Jobs.Cancel(projectID, jobReferenceID)
	cancelCall.Location(location)
	_, _ = cancelCall.Context(ctx).Do()
}
