package bench

/*

commented out to reduce gomod dep

import (
	"cloud.google.com/go/bigquery"
	"context"
	"google.golang.org/api/iterator"
	"time"
)




func testGCPClientQuery(projectID, query string, repeat int, values []interface{}) (time.Duration, error) {
	started := time.Now()
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return 0, err
	}
	defer client.Close()
	for i := 0; i < repeat; i++ {
		q := client.Query(query)
		// Location must match that of the dataset(s) referenced in the query.
		q.Location = "US"
		var row []bigquery.Value  = make([]bigquery.Value, len(values))
		for i, r := range values {
			row[i] = r
		}
		// Run the query and print results when the query job is completed.
		job, err := q.Run(ctx)
		if err != nil {
			return 0, err
		}
		it, err := job.Read(ctx)
		if err != nil {
			return 0, err
		}
		for {
			err = it.Next(&row)
			if err == iterator.Done {
				break
			}
			for i := range row {
				values[i] = row[i]
			}
		}
	}
	return time.Now().Sub(started), nil
}


*/