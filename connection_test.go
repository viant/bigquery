package bigquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJobConfiguration_Reservation(t *testing.T) {
	var testCases = []struct {
		description         string
		cfg                 *Config
		query               string
		expectedReservation string
		expectedPriority    string
	}{
		{
			description: "reservation set from config",
			cfg: &Config{
				ProjectID:   "myproject",
				DatasetID:   "mydataset",
				Priority:    PriorityInteractive,
				Reservation: "projects/myproject/locations/us/reservations/myreservation",
			},
			query:               "SELECT 1",
			expectedReservation: "projects/myproject/locations/us/reservations/myreservation",
			expectedPriority:    PriorityInteractive,
		},
		{
			description: "no reservation when not configured",
			cfg: &Config{
				ProjectID: "myproject",
				DatasetID: "mydataset",
				Priority:  PriorityInteractive,
			},
			query:               "SELECT 1",
			expectedReservation: "",
			expectedPriority:    PriorityInteractive,
		},
		{
			description: "reservation with batch priority",
			cfg: &Config{
				ProjectID:   "myproject",
				DatasetID:   "mydataset",
				Priority:    PriorityBatch,
				Reservation: "projects/myproject/locations/us/reservations/prod",
			},
			query:               "SELECT 1",
			expectedReservation: "projects/myproject/locations/us/reservations/prod",
			expectedPriority:    PriorityBatch,
		},
		{
			description: "priority from hint overrides config priority but reservation still set",
			cfg: &Config{
				ProjectID:   "myproject",
				DatasetID:   "mydataset",
				Priority:    PriorityInteractive,
				Reservation: "projects/myproject/locations/us/reservations/myreservation",
			},
			query:               `SELECT /*+ {"Priority": "BATCH"} +*/ 1`,
			expectedReservation: "projects/myproject/locations/us/reservations/myreservation",
			expectedPriority:    PriorityBatch,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			conn := &connection{cfg: tc.cfg, projectID: tc.cfg.ProjectID}
			job, err := conn.jobConfiguration(tc.query)
			if !assert.NoError(t, err) {
				return
			}
			assert.Equal(t, tc.expectedReservation, job.Configuration.Reservation)
			assert.Equal(t, tc.expectedPriority, job.Configuration.Query.Priority)
		})
	}
}
