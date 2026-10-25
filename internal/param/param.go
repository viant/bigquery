package param

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"math/big"
	"reflect"
	"time"
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
	val := reflect.ValueOf(p.value)
	// SQLX binds addresses of nullable fields, yielding pointer chains. Keep
	// the final typed pointer so a nil chain retains its BigQuery SQL type.
	for val.IsValid() && val.Kind() == reflect.Ptr && val.Type().Elem().Kind() == reflect.Ptr {
		if val.IsNil() {
			val = reflect.Zero(val.Type().Elem())
		} else {
			val = val.Elem()
		}
	}
	if !val.IsValid() {
		return nil, fmt.Errorf("cannot encode untyped NULL query parameter: explicit BigQuery type required")
	}
	if typed, ok := val.Interface().(typedValue); ok {
		if val.Kind() == reflect.Ptr && val.IsNil() {
			return nil, fmt.Errorf("nil typed SQL parameter wrapper")
		}
		return typedParameter(p.Name, typed)
	}
	if raw, ok := val.Interface().(json.RawMessage); ok {
		return jsonParameter(p.Name, raw)
	}
	if val.Kind() == reflect.Ptr && val.Type().Elem() == reflect.TypeFor[json.RawMessage]() {
		if val.IsNil() {
			return jsonParameter(p.Name, nil)
		}
		return jsonParameter(p.Name, val.Elem().Interface().(json.RawMessage))
	}
	if value, ok := val.Interface().(driver.Valuer); ok {
		if val.Kind() == reflect.Ptr && val.IsNil() {
			return nil, fmt.Errorf("cannot encode nil query parameter of type %s: explicit BigQuery type required", val.Type())
		}
		// JSON-aware SQL adapters retain JSON semantics without coupling the
		// driver to their packages. Ordinary Valuers retain scalar behavior.
		if marshaler, ok := value.(json.Marshaler); ok {
			raw, err := marshaler.MarshalJSON()
			if err != nil {
				return nil, err
			}
			return jsonParameter(p.Name, raw)
		}
		scalar, err := value.Value()
		if err != nil {
			return nil, err
		}
		return New(p.Name, scalar).QueryParameter()
	}
	fn := values[val.Kind()]
	if fn == nil || values[valueKey(val.Type())] == nil {
		return nil, fmt.Errorf("unsupported type: %v", val.Type().String())
	}
	if val.Kind() == reflect.Ptr && val.IsNil() {
		// Avoid unsafe pointer arithmetic on nil in the value encoders.
		switch val.Type().Elem().Kind() {
		case reflect.Bool:
			return NewBoolPtrQueryParameter(p.Name, nil)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return NewIntPtrQueryParameter(p.Name, nil)
		case reflect.Float32, reflect.Float64:
			return NewFloatPtrQueryParameter(p.Name, nil)
		case reflect.String:
			return NewStringPtrQueryParameter(p.Name, nil)
		case reflect.Struct:
			switch val.Type().Elem() {
			case reflect.TypeOf(time.Time{}):
				return NewTimeQueryParameter(p.Name, nil)
			case reflect.TypeOf(big.Rat{}):
				return NewBigNumericQueryParameter(p.Name, nil)
			}
		}
		return nil, fmt.Errorf("cannot encode nil query parameter of type %s: unsupported typed NULL", val.Type())
	}
	ptr := reflect.New(val.Type())
	ptr.Elem().Set(val)
	return fn(reflect.StructField{Name: p.Name, Type: val.Type()}, unsafe.Pointer(ptr.Pointer()))
}

// New creates a param
func New(name string, value interface{}) *Param {
	return &Param{Name: name, value: value}
}

func jsonParameter(name string, raw json.RawMessage) (*bigquery.QueryParameter, error) {
	result := &bigquery.QueryParameter{Name: name, ParameterType: &bigquery.QueryParameterType{Type: "JSON"}, ParameterValue: &bigquery.QueryParameterValue{}}
	if raw == nil {
		return result, nil
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("invalid JSON query parameter")
	}
	result.ParameterValue.Value = string(raw)
	return result, nil
}
