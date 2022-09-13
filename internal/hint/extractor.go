package hint

import "strings"

// Extract extracts json formatted hint from sql statement
func Extract(query string) string {
	if index := strings.Index(query, "/*+"); index != -1 {
		if end := strings.Index(query, "+*/"); end != -1 {
			hint := strings.TrimSpace(query[index+3 : end])
			if strings.HasPrefix(hint, "{") || strings.HasSuffix(hint, "}") {
				return hint
			}
		}
	}
	return ""
}
