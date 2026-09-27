package param

import (
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
	if !val.IsValid() {
		return nil, fmt.Errorf("cannot encode untyped NULL query parameter: explicit BigQuery type required")
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
