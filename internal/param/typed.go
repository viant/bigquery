package param

import (
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"reflect"
	"strings"
	"time"
)

type typedValue interface {
	SQLType() string
	SQLValue() any
}

func typedParameter(name string, value typedValue) (*bigquery.QueryParameter, error) {
	kind := strings.ToUpper(value.SQLType())
	layout := ""
	switch kind {
	case "DATE":
		layout = "2006-01-02"
	case "TIME":
		layout = "15:04:05.999999"
	case "DATETIME":
		layout = "2006-01-02 15:04:05.999999"
	case "TIMESTAMP":
		layout = time.RFC3339Nano
	default:
		return nil, fmt.Errorf("unsupported explicit SQL parameter type %s", kind)
	}
	result := &bigquery.QueryParameter{Name: name, ParameterType: &bigquery.QueryParameterType{Type: kind}, ParameterValue: &bigquery.QueryParameterValue{}}
	v := reflect.ValueOf(value.SQLValue())
	for v.IsValid() && (v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface) {
		if v.IsNil() {
			return result, nil
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return result, nil
	}
	switch scalar := v.Interface().(type) {
	case time.Time:
		result.ParameterValue.Value = scalar.Format(layout)
	case string:
		result.ParameterValue.Value = scalar
		result.ParameterValue.ForceSendFields = []string{"Value"}
	default:
		return nil, fmt.Errorf("unsupported %s parameter value type %s", kind, v.Type())
	}
	return result, nil
}
