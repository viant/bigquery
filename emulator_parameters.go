package bigquery

import (
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"strings"
)

// emulatorParameters uses GoogleSQL named bindings because the emulator can
// misnumber positional bindings inside aggregate expressions. Values are never
// interpolated, and production connections retain their existing wire queries.
func emulatorParameters(query string) (string, []string, error) {
	var out strings.Builder
	var names []string
	named := false
	for i := 0; i < len(query); {
		start := i
		switch {
		case query[i] == '\'' || query[i] == '"' || query[i] == '`':
			quote := query[i]
			size := 1
			if i+2 < len(query) && query[i+1] == quote && query[i+2] == quote && quote != '`' {
				size = 3
			}
			delimiter := strings.Repeat(string(quote), size)
			i += size
			closed := false
			for i < len(query) {
				if query[i] == '\\' {
					i += 2
					continue
				}
				if strings.HasPrefix(query[i:], delimiter) {
					i += size
					closed = true
					break
				}
				i++
			}
			if !closed {
				return "", nil, fmt.Errorf("unterminated quoted SQL in emulator query")
			}
			out.WriteString(query[start:i])
		case query[i] == '#' || strings.HasPrefix(query[i:], "--"):
			for i < len(query) && query[i] != '\n' {
				i++
			}
			out.WriteString(query[start:i])
		case strings.HasPrefix(query[i:], "/*"):
			end := strings.Index(query[i+2:], "*/")
			if end < 0 {
				return "", nil, fmt.Errorf("unterminated SQL comment in emulator query")
			}
			i += end + 4
			out.WriteString(query[start:i])
		case query[i] == '?':
			name := fmt.Sprintf("viantPosition%d", len(names)+1)
			names = append(names, name)
			out.WriteByte('@')
			out.WriteString(name)
			i++
		default:
			if query[i] == '@' && (i == 0 || query[i-1] != '@') && i+1 < len(query) && query[i+1] != '@' {
				named = true
			}
			out.WriteByte(query[i])
			i++
		}
	}
	if named && len(names) > 0 {
		return "", nil, fmt.Errorf("cannot mix named and positional query parameters")
	}
	return out.String(), names, nil
}

func (s *Statement) bindParameters(params []*bigquery.QueryParameter) error {
	if len(s.parameterNames) == 0 {
		s.job.Configuration.Query.QueryParameters = params
		return nil
	}
	if len(params) != len(s.parameterNames) {
		return fmt.Errorf("expected %d query parameters, got %d", len(s.parameterNames), len(params))
	}
	bound := make([]*bigquery.QueryParameter, len(params))
	for i, p := range params {
		if p == nil {
			return fmt.Errorf("query parameter %d is missing", i+1)
		}
		copy := *p
		copy.Name = s.parameterNames[i]
		bound[i] = &copy
	}
	s.job.Configuration.Query.QueryParameters = bound
	return nil
}
