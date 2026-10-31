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

func TestPreparedQueryKeepsDefaultDatasetAndJobProjectSeparate(t *testing.T) {
	for _, fixture := range []struct {
		name        string
		jobProject  string
		dataProject string
		dataset     string
	}{
		{name: "same project", jobProject: "data", dataProject: "data", dataset: "records"},
		{name: "separate job project", jobProject: "billing", dataProject: "data", dataset: "records"},
		{name: "no default dataset", jobProject: "billing", dataProject: "data"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			conn := &connection{projectID: fixture.jobProject, cfg: &Config{ProjectID: fixture.dataProject, DatasetID: fixture.dataset, Priority: PriorityInteractive}}
			prepared, err := conn.PrepareContext(t.Context(), "SELECT 1")
			if !assert.NoError(t, err) {
				return
			}
			defer prepared.Close()
			statement, ok := prepared.(*Statement)
			if !assert.True(t, ok, "query must prepare a query statement") {
				return
			}
			assert.Equal(t, fixture.jobProject, statement.projectID, "job submission must keep its connection-owned project")
			dataset := statement.job.Configuration.Query.DefaultDataset
			if fixture.dataset == "" {
				assert.Nil(t, dataset)
				return
			}
			if assert.NotNil(t, dataset) {
				assert.Equal(t, fixture.dataProject, dataset.ProjectId, "unqualified tables must resolve in the configured data project")
				assert.Equal(t, fixture.dataset, dataset.DatasetId)
			}
		})
	}
}
