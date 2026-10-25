package bigquery

import (
	"fmt"
	"strconv"
	"strings"

	bq "google.golang.org/api/bigquery/v2"
)

// emulatorParameters uses equivalent named bindings for the local emulator.
// The emulator can misorder positional inputs around IN and DATE expressions.
// Only placeholder tokens change; SQL literals, identifiers and comments do not.
func emulatorParameters(query string) (string, []string, error) {
	return rewriteEmulatorParameters(query, nil)
}

func rewriteEmulatorParameters(query string, jsonNames map[string]bool) (string, []string, error) {
	var out strings.Builder
	var names []string
	named := false
	for i := 0; i < len(query); {
		start := i
		switch {
		case query[i] == '\'' || query[i] == '"' || query[i] == '`':
			quote := query[i]
			width := 1
			if quote != '`' && i+2 < len(query) && query[i+1] == quote && query[i+2] == quote {
				width = 3
			}
			i += width
			for i < len(query) {
				if query[i] == '\\' {
					i++
					if i < len(query) {
						i++
					}
					continue
				}
				if query[i] == quote {
					if width == 3 {
						if i+2 < len(query) && query[i+1] == quote && query[i+2] == quote {
							i += 3
							break
						}
					} else {
						i++
						if i < len(query) && query[i] == quote {
							i++
							continue
						}
						break
					}
				}
				i++
			}
			out.WriteString(query[start:i])
		case query[i] == '#' || (query[i] == '-' && i+1 < len(query) && query[i+1] == '-'):
			for i < len(query) && query[i] != '\n' {
				i++
			}
			out.WriteString(query[start:i])
		case query[i] == '/' && i+1 < len(query) && query[i+1] == '*':
			i += 2
			for i+1 < len(query) && !(query[i] == '*' && query[i+1] == '/') {
				i++
			}
			if i+1 < len(query) {
				i += 2
			} else {
				i = len(query)
			}
			out.WriteString(query[start:i])
		case query[i] == '?':
			name := "viant_emulator_p" + strconv.Itoa(len(names)+1)
			names = append(names, name)
			out.WriteByte('@')
			out.WriteString(name)
			i++
		case query[i] == '@':
			if i+1 < len(query) && query[i+1] == '@' {
				out.WriteString("@@")
				i += 2
				continue
			}
			if i+1 < len(query) && ((query[i+1] >= 'a' && query[i+1] <= 'z') || (query[i+1] >= 'A' && query[i+1] <= 'Z') || query[i+1] == '_') {
				named = true
				end := i + 2
				for end < len(query) && ((query[end] >= 'a' && query[end] <= 'z') || (query[end] >= 'A' && query[end] <= 'Z') || (query[end] >= '0' && query[end] <= '9') || query[end] == '_') {
					end++
				}
				name := query[i+1 : end]
				if jsonNames[name] {
					out.WriteString("PARSE_JSON(" + query[i:end] + ")")
				} else {
					out.WriteString(query[i:end])
				}
				i = end
				continue
			}
			out.WriteByte(query[i])
			i++
		default:
			out.WriteByte(query[i])
			i++
		}
	}
	if named && len(names) > 0 {
		return "", nil, fmt.Errorf("BigQuery cannot mix named and positional parameters")
	}
	return out.String(), names, nil
}

func (s *Statement) bindParameters(params []*bq.QueryParameter) error {
	if len(s.emulatorParameterNames) == 0 {
		return s.bindEmulatorJSON(params)
	}
	if len(params) != len(s.emulatorParameterNames) {
		return fmt.Errorf("emulator parameter count: got %d, expected %d", len(params), len(s.emulatorParameterNames))
	}
	bound := make([]*bq.QueryParameter, len(params))
	for i, p := range params {
		if p == nil || p.Name != "" {
			return fmt.Errorf("emulator positional parameter %d must be unnamed", i+1)
		}
		clone := *p
		clone.Name = s.emulatorParameterNames[i]
		bound[i] = &clone
	}
	s.job.Configuration.Query.ParameterMode = "NAMED"
	return s.bindEmulatorJSON(bound)
}

// The emulator currently treats JSON parameters as STRING. Preserve JSON
// semantics with PARSE_JSON only in emulator mode; production stays typed JSON.
func (s *Statement) bindEmulatorJSON(params []*bq.QueryParameter) error {
	if s.emulatorQuery == "" {
		s.job.Configuration.Query.QueryParameters = params
		return nil
	}
	bound := make([]*bq.QueryParameter, len(params))
	names := map[string]bool{}
	for i, p := range params {
		if p == nil {
			bound[i] = p
			continue
		}
		clone := *p
		if p.ParameterType != nil && p.ParameterType.Type == "JSON" {
			clone.ParameterType = &bq.QueryParameterType{Type: "STRING"}
			names[p.Name] = true
		}
		bound[i] = &clone
	}
	query, _, err := rewriteEmulatorParameters(s.emulatorQuery, names)
	if err != nil {
		return err
	}
	s.job.Configuration.Query.Query = query
	s.job.Configuration.Query.QueryParameters = bound
	return nil
}
