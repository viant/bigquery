package param

import (
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

//Param represents a query param
type Param struct {
	Name  string
	value interface{}
}

//QueryParameter returns bigquery QueryParameter
func (p *Param) QueryParameter() (*bigquery.QueryParameter, error) {
	val := reflect.ValueOf(p.value)
	var ptr reflect.Value
	fn := values[val.Kind()]
	if val.Kind() == reflect.Ptr {
		ptr = val
	} else {
		ptr = reflect.New(val.Type())
		ptr.Elem().Set(val)
	}

	if fn == nil {
		return nil, fmt.Errorf("unsupported type: %v", val.Type().String())
	}
	return fn(reflect.StructField{Name: p.Name, Type: val.Type()}, unsafe.Pointer(ptr.Pointer()))
}

//New creates a param
func New(name string, value interface{}) *Param {
	return &Param{Name: name, value: value}
}
