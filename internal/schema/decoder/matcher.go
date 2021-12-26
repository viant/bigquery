package decoder

import (
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"reflect"
	"strings"
)

//TODO move it to xunsafe under struct
func matchFields(dest reflect.Type, owner *bigquery.TableFieldSchema) ([]*reflect.StructField, error) {
	var exactMap = map[string]*reflect.StructField{}
	var fuzzyMap = map[string]*reflect.StructField{}

	structType := dest
	if structType.Kind() == reflect.Ptr {
		structType = structType.Elem()
	}
	var result = make([]*reflect.StructField, len(owner.Fields))
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		exactMap[field.Name] = &field
		fuzzyMap[normalizeForFuzzyMatch(field.Name)] = &field
	}
	for i, candidate := range owner.Fields {
		field, ok := exactMap[candidate.Name]
		if ok {
			result[i] = field
			continue
		}
		if field, ok = fuzzyMap[normalizeForFuzzyMatch(candidate.Name)]; !ok {
			return nil, fmt.Errorf("failed to match %v.%v with %v", owner.Type, candidate.Name, structType.String())
		}
		result[i] = field
	}
	return result, nil
}


func normalizeForFuzzyMatch(name string) string {
	result := strings.ToLower(name)
	if index := strings.Index(result, "_");index != -1 {
		result = strings.Replace(result, "_", "", strings.Count(result, "_"))
	}
	return result
}

