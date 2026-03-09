package bigquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseDSN(t *testing.T) {
	var testCases = []struct {
		description string
		dsn         string
		expect      Config
		expectError bool
	}{
		{
			description: "basic DSN with project and dataset",
			dsn:         "bigquery://myproject/us/mydataset",
			expect: Config{
				ProjectID: "myproject",
				DatasetID: "mydataset",
				Location:  "us",
				App:       defaultApp,
				Priority:  PriorityInteractive,
			},
		},
		{
			description: "DSN with batch priority",
			dsn:         "bigquery://myproject/us/mydataset?priority=batch",
			expect: Config{
				ProjectID: "myproject",
				DatasetID: "mydataset",
				Location:  "us",
				App:       defaultApp,
				Priority:  PriorityBatch,
			},
		},
		{
			description: "DSN with reservation",
			dsn:         "bigquery://myproject/us/mydataset?reservation=projects/myproject/locations/us/reservations/myreservation",
			expect: Config{
				ProjectID:   "myproject",
				DatasetID:   "mydataset",
				Location:    "us",
				App:         defaultApp,
				Priority:    PriorityInteractive,
				Reservation: "projects/myproject/locations/us/reservations/myreservation",
			},
		},
		{
			description: "DSN with reservation and priority",
			dsn:         "bigquery://myproject/us/mydataset?reservation=projects/myproject/locations/us/reservations/myreservation&priority=batch",
			expect: Config{
				ProjectID:   "myproject",
				DatasetID:   "mydataset",
				Location:    "us",
				App:         defaultApp,
				Priority:    PriorityBatch,
				Reservation: "projects/myproject/locations/us/reservations/myreservation",
			},
		},
		{
			description: "DSN without location defaults to us",
			dsn:         "bigquery://myproject/mydataset",
			expect: Config{
				ProjectID: "myproject",
				DatasetID: "mydataset",
				Location:  "us",
				App:       defaultApp,
				Priority:  PriorityInteractive,
			},
		},
		{
			description: "invalid scheme",
			dsn:         "postgres://myproject/mydataset",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			cfg, err := ParseDSN(tc.dsn)
			if tc.expectError {
				assert.Error(t, err)
				return
			}
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, tc.expect.ProjectID, cfg.ProjectID)
			assert.Equal(t, tc.expect.DatasetID, cfg.DatasetID)
			assert.Equal(t, tc.expect.Location, cfg.Location)
			assert.Equal(t, tc.expect.Priority, cfg.Priority)
			assert.Equal(t, tc.expect.Reservation, cfg.Reservation)
			assert.Equal(t, tc.expect.App, cfg.App)
		})
	}
}
