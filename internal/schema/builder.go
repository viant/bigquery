package schema

import (
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"reflect"
	"strings"
)

// BuildSchemaTypes build type matching table schema
func BuildSchemaTypes(table *bigquery.TableSchema) ([]reflect.Type, error) {
	var result = make([]reflect.Type, len(table.Fields))
	var err error
	for i, field := range table.Fields {
		if result[i], err = BuildFieldType(field); err != nil {
			return nil, err
		}
	}
	return result, nil
}

type loc struct{}

var location = reflect.TypeOf(&loc{}).PkgPath()

// BuildFieldType build a field type from big query schema
func BuildFieldType(field *bigquery.TableFieldSchema) (reflect.Type, error) {
	var dataType reflect.Type
	var err error
	if len(field.Fields) == 0 {
		if dataType, err = mapBasicType(field.Type, field.Mode == "NULLABLE"); err != nil {
			return nil, fmt.Errorf("failed to build field: %v, %w", field.Name, err)
		}
	} else {
		var structFields = make([]reflect.StructField, len(field.Fields))
		for i, subField := range field.Fields {
			fieldType, err := BuildFieldType(subField)
			if err != nil {
				return nil, fmt.Errorf("failed to build fieldType: %v, %w", field.Name, err)
			}
			omitEmpty := ""
			if subField.Mode == "NULLABLE" {
				omitEmpty = ",omitempty"
			}
			structFields[i] = reflect.StructField{
				Name:    strings.Title(subField.Name),
				Type:    fieldType,
				Tag:     reflect.StructTag(fmt.Sprintf(`json:"%v%v"`, subField.Name, omitEmpty)),
				PkgPath: location,
			}

		}
		dataType = reflect.StructOf(structFields)
	}
	if isRepeated := field.Mode == "REPEATED"; isRepeated {
		dataType = reflect.SliceOf(dataType)
	}
	return dataType, nil
}
