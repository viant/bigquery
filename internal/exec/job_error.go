package exec

import (
	"encoding/json"
	"fmt"
	"strings"

	"google.golang.org/api/bigquery/v2"
)

// TerminalJobError retains the structured terminal job reason for trusted callers.
// Its text remains the native driver error; applications must project only
// allowlisted diagnostics rather than serialize warehouse messages.
type TerminalJobError struct {
	reason   string
	text     string
	message  string
	location string
}

func (e *TerminalJobError) Error() string { return e.text }

// BigQueryReason exposes only the native ErrorProto reason, independently of
// message text, SQL and credentials. Consumers must allowlist recognized values.
func (e *TerminalJobError) BigQueryReason() string { return e.reason }

// BigQueryMessage and BigQueryLocation retain provider detail for authorized
// operator interfaces. Callers must redact credentials before displaying it.
func (e *TerminalJobError) BigQueryMessage() string  { return e.message }
func (e *TerminalJobError) BigQueryLocation() string { return e.location }

func newTerminalJobError(status *bigquery.JobStatus) *TerminalJobError {
	raw, _ := json.Marshal(status.Errors)
	messages := []string{}
	seen := map[string]bool{}
	for _, item := range append([]*bigquery.ErrorProto{status.ErrorResult}, status.Errors...) {
		if item != nil && item.Message != "" && !seen[item.Message] {
			messages = append(messages, item.Message)
			seen[item.Message] = true
		}
	}
	return &TerminalJobError{reason: status.ErrorResult.Reason, text: fmt.Sprintf("%v: %s", status.ErrorResult.Message, raw), message: strings.Join(messages, "\n"), location: status.ErrorResult.Location}
}
