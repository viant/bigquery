package decoder

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"google.golang.org/api/bigquery/v2"
	"reflect"
	"unsafe"
)

type valueSlice struct {
	slice *xunsafe.Slice
	ptr         unsafe.Pointer
	appender    *xunsafe.Appender
	unmarshaler Unmarshaler
	newUnmarshaler
}

func (o *valueSlice) UnmarshalJSONArray(dec *gojay.Decoder) error {
	ptr := o.appender.Add()
	if o.unmarshaler == nil {
		o.unmarshaler = o.newUnmarshaler(ptr)
	} else {
		o.unmarshaler.set(ptr)
	}
	return dec.Object(o.unmarshaler)
}

func (o *valueSlice) set(value interface{}) {
	ptr := xunsafe.AsPointer(value)
	o.ptr = ptr
	o.appender = o.slice.Appender(ptr)
}

func (o *valueSlice) UnmarshalJSONObject(dec *gojay.Decoder, _ string) error {
	return dec.Array(o)
}

func (o *valueSlice) NKeys() int {
	return 1
}

func newValueSliceUnmarshaler(field *bigquery.TableFieldSchema, dest reflect.Type) (func(ptr interface{}) Unmarshaler, error) {
	newUnmarshaler, err := newJSONUnmarshaler(&bigquery.TableFieldSchema{
		Mode: "NULLABLE",
		Name: field.Name,
		Type: field.Type,
		Fields: field.Fields,
	}, dest.Elem())
	if err != nil {
		return nil, err
	}
	aSlice := xunsafe.NewSlice(dest)
	return func(value interface{}) Unmarshaler {
		ptr := xunsafe.AsPointer(value)
		result := &valueSlice{ptr: ptr, appender: aSlice.Appender(ptr), slice: aSlice}
		result.newUnmarshaler = newUnmarshaler
		return result
	}, nil
}
