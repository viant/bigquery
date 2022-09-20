package ingestion

import (
	"bytes"
	"fmt"
	"strings"
)

func extractJSONKeyValue(data []byte, key string) (string, error) {
	match := `"` + key + `":`
	offset := bytes.Index(data, []byte(match))
	if offset == -1 {
		return "", fmt.Errorf("failed to locate: %v", key)
	}
	var limit = 0
outer:
	for limit = offset + len(match); limit < len(data); limit++ {
		c := data[limit]
		switch c {
		case ',', '}':
			break outer
		}
	}
	value := strings.TrimSpace(string(data[offset+len(match) : limit]))
	if len(value) > 0 && value[0] == '"' {
		value = value[1 : len(value)-2]
	}
	return value, nil
}
