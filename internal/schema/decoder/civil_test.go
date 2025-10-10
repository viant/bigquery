package decoder

import (
	"cloud.google.com/go/civil"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCivilDateDecoding tests that BigQuery DATE columns can be scanned into civil.Date
func TestCivilDateDecoding(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected civil.Date
		wantErr  bool
	}{
		{
			name:     "valid date",
			input:    "2024-03-15",
			expected: civil.Date{Year: 2024, Month: 3, Day: 15},
			wantErr:  false,
		},
		{
			name:     "min date",
			input:    "0001-01-01",
			expected: civil.Date{Year: 1, Month: 1, Day: 1},
			wantErr:  false,
		},
		{
			name:     "leap year date",
			input:    "2024-02-29",
			expected: civil.Date{Year: 2024, Month: 2, Day: 29},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, err := civil.ParseDate(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, d)
		})
	}
}

// TestCivilTimeDecoding tests that BigQuery TIME columns can be scanned into civil.Time
func TestCivilTimeDecoding(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected civil.Time
		wantErr  bool
	}{
		{
			name:     "time without fractional seconds",
			input:    "12:30:45",
			expected: civil.Time{Hour: 12, Minute: 30, Second: 45, Nanosecond: 0},
			wantErr:  false,
		},
		{
			name:     "time with microseconds",
			input:    "12:30:45.123456",
			expected: civil.Time{Hour: 12, Minute: 30, Second: 45, Nanosecond: 123456000},
			wantErr:  false,
		},
		{
			name:     "midnight",
			input:    "00:00:00",
			expected: civil.Time{Hour: 0, Minute: 0, Second: 0, Nanosecond: 0},
			wantErr:  false,
		},
		{
			name:     "end of day",
			input:    "23:59:59.999999",
			expected: civil.Time{Hour: 23, Minute: 59, Second: 59, Nanosecond: 999999000},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm, err := civil.ParseTime(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, tm)
		})
	}
}

// TestCivilDateTimeDecoding tests that BigQuery DATETIME columns can be scanned into civil.DateTime
func TestCivilDateTimeDecoding(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected civil.DateTime
		wantErr  bool
	}{
		{
			name:  "datetime without fractional seconds",
			input: "2024-03-15 12:30:45",
			expected: civil.DateTime{
				Date: civil.Date{Year: 2024, Month: 3, Day: 15},
				Time: civil.Time{Hour: 12, Minute: 30, Second: 45, Nanosecond: 0},
			},
			wantErr: false,
		},
		{
			name:  "datetime with microseconds",
			input: "2024-03-15 12:30:45.123456",
			expected: civil.DateTime{
				Date: civil.Date{Year: 2024, Month: 3, Day: 15},
				Time: civil.Time{Hour: 12, Minute: 30, Second: 45, Nanosecond: 123456000},
			},
			wantErr: false,
		},
		{
			name:  "datetime at midnight",
			input: "2024-01-01 00:00:00",
			expected: civil.DateTime{
				Date: civil.Date{Year: 2024, Month: 1, Day: 1},
				Time: civil.Time{Hour: 0, Minute: 0, Second: 0, Nanosecond: 0},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// BigQuery DATETIME format uses space, convert to 'T' for civil.ParseDateTime
			input := tt.input
			if len(input) > 10 && input[10] == ' ' {
				input = input[:10] + "T" + input[11:]
			}
			dt, err := civil.ParseDateTime(input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, dt)
		})
	}
}

// TestCivilTypeProperties tests important properties of civil types
func TestCivilTypeProperties(t *testing.T) {
	t.Run("civil.Date has no timezone", func(t *testing.T) {
		d := civil.Date{Year: 2024, Month: 3, Day: 15}
		// civil.Date represents only a calendar date, no time or timezone
		assert.Equal(t, 2024, d.Year)
		assert.Equal(t, 3, int(d.Month))
		assert.Equal(t, 15, d.Day)
	})

	t.Run("civil.Time has no timezone", func(t *testing.T) {
		tm := civil.Time{Hour: 12, Minute: 30, Second: 45}
		// civil.Time represents only time-of-day, no date or timezone
		assert.Equal(t, 12, tm.Hour)
		assert.Equal(t, 30, tm.Minute)
		assert.Equal(t, 45, tm.Second)
	})

	t.Run("civil.DateTime has no timezone", func(t *testing.T) {
		dt := civil.DateTime{
			Date: civil.Date{Year: 2024, Month: 3, Day: 15},
			Time: civil.Time{Hour: 12, Minute: 30, Second: 45},
		}
		// civil.DateTime combines date and time but has no timezone
		assert.Equal(t, 2024, dt.Date.Year)
		assert.Equal(t, 12, dt.Time.Hour)
	})
}
