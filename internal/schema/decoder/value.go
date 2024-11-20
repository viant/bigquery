package decoder

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"google.golang.org/api/bigquery/v2"
	"reflect"
	"unsafe"
)

// value represents basic value unmarshaler
type value struct {
	name   string
	rawPtr interface{}
	ptr    unsafe.Pointer
	decode func(dec *gojay.Decoder, dest unsafe.Pointer) error
}

// UnmarshalJSONObject decode JSON object
func (o *value) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {
	return o.decode(dec, o.ptr)
}

func (o *value) set(ptr interface{}) {
	o.ptr = xunsafe.AsPointer(ptr)
	o.rawPtr = ptr
}

func (o *value) value() interface{} {
	return o.rawPtr
}

// NKeys returns max of expected keys
func (o *value) NKeys() int {
	return 1
}

func newValueUnmarshaler(field *bigquery.TableFieldSchema, dest reflect.Type) (func(ptr interface{}) Unmarshaler, error) {
	decode, err := baseUnmarshaler(field.Type, dest)
	if err != nil {
		return nil, err
	}
	return func(ptr interface{}) Unmarshaler {
		return &value{name: field.Name, rawPtr: ptr, ptr: xunsafe.AsPointer(ptr), decode: decode}
	}, nil
}
