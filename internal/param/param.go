package param

import (
	"database/sql/driver"
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"reflect"
	"unsafe"
)

var (
	paramTypeInt        = &bigquery.QueryParameterType{Type: "INT64"}
	paramTypeFloat64    = &bigquery.QueryParameterType{Type: "FLOAT64"}
	paramTypeBool       = &bigquery.QueryParameterType{Type: "BOOL"}
	paramTypeString     = &bigquery.QueryParameterType{Type: "STRING"}
	paramTypeBytes      = &bigquery.QueryParameterType{Type: "BYTES"}
	paramTypeDate       = &bigquery.QueryParameterType{Type: "DATE"}
	paramTypeTime       = &bigquery.QueryParameterType{Type: "TIME"}
	paramTypeDateTime   = &bigquery.QueryParameterType{Type: "DATETIME"}
	paramTypeTimestamp  = &bigquery.QueryParameterType{Type: "TIMESTAMP"}
	paramTypeNumeric    = &bigquery.QueryParameterType{Type: "NUMERIC"}
	paramTypeBigNumeric = &bigquery.QueryParameterType{Type: "BIGNUMERIC"}
)

// Param represents a query param
type Param struct {
	Name  string
	value interface{}
}

// QueryParameter returns bigquery QueryParameter
func (p *Param) QueryParameter() (*bigquery.QueryParameter, error) {
	value := p.value

	// Check if the value implements driver.Valuer interface
	if valuer, ok := value.(driver.Valuer); ok {
		var err error
		value, err = valuer.Value()
		if err != nil {
			return nil, fmt.Errorf("failed to get value from driver.Valuer: %w", err)
		}
		// If Value() returns nil, we need to determine the appropriate null parameter type
		// by examining the original type
		if value == nil {
			return createNullParameter(p.Name, p.value)
		}
	}

	val := reflect.ValueOf(value)
	var ptr reflect.Value
	fn := values[val.Kind()]
	ptr = reflect.New(val.Type())
	ptr.Elem().Set(val)
	if fn == nil {
		return nil, fmt.Errorf("unsupported type: %v", val.Type().String())
	}
	return fn(reflect.StructField{Name: p.Name, Type: val.Type()}, unsafe.Pointer(ptr.Pointer()))
}

// createNullParameter creates a null parameter with the appropriate type based on the original value
func createNullParameter(name string, originalValue interface{}) (*bigquery.QueryParameter, error) {
	// Use reflection to infer the underlying type from sql.Null* struct fields
	t := reflect.TypeOf(originalValue)
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct type for null value, got: %v", t.Kind())
	}

	// sql.Null* types have a value field that indicates the underlying type
	// For example, sql.NullBool has Bool field, sql.NullInt64 has Int64 field, etc.
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		// Skip the Valid field which all sql.Null* types have
		if field.Name == "Valid" {
			continue
		}

		// Infer the type from the field kind
		switch field.Type.Kind() {
		case reflect.Bool:
			return NewBoolPtrQueryParameter(name, nil)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return NewIntPtrQueryParameter(name, nil)
		case reflect.Float32, reflect.Float64:
			return NewFloatPtrQueryParameter(name, nil)
		case reflect.String:
			return NewStringPtrQueryParameter(name, nil)
		case reflect.Struct:
			// Check if it's a time.Time
			if field.Type.String() == "time.Time" {
				return NewTimeQueryParameter(name, nil)
			}
		}
	}

	return nil, fmt.Errorf("unable to determine null parameter type for: %v", t.String())
}

// New creates a param
func New(name string, value interface{}) *Param {
	return &Param{Name: name, value: value}
}
