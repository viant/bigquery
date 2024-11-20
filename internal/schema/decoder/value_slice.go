package decoder

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"google.golang.org/api/bigquery/v2"
	"reflect"
	"unsafe"
)

type valueSlice struct {
	fieldName   string
	rawPtr      interface{}
	slice       *xunsafe.Slice
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
	err := dec.Object(o.unmarshaler)
	return err
}

func (o *valueSlice) value() interface{} {
	return o.rawPtr
}

func (o *valueSlice) set(rawPtr interface{}) {
	o.rawPtr = rawPtr
	ptr := xunsafe.AsPointer(rawPtr)
	o.ptr = ptr
	o.appender = o.slice.Appender(ptr)
	//o.appender.Trunc(0)
}

func (o *valueSlice) UnmarshalJSONObject(dec *gojay.Decoder, k string) error {
	return dec.Array(o)
}

func (o *valueSlice) NKeys() int {
	return 1
}

func newValueSliceUnmarshaler(field *bigquery.TableFieldSchema, dest reflect.Type) (func(ptr interface{}) Unmarshaler, error) {
	newUnmarshaler, err := newJSONUnmarshaler(&bigquery.TableFieldSchema{
		Mode:   "NULLABLE",
		Name:   field.Name,
		Type:   field.Type,
		Fields: field.Fields,
	}, dest.Elem())
	if err != nil {
		return nil, err
	}
	aSlice := xunsafe.NewSlice(dest)
	return func(rawPtr interface{}) Unmarshaler {
		ptr := xunsafe.AsPointer(rawPtr)
		appender := aSlice.Appender(ptr)
		result := &valueSlice{rawPtr: rawPtr, ptr: ptr, appender: appender, slice: aSlice, fieldName: field.Name}
		result.newUnmarshaler = newUnmarshaler
		return result
	}, nil
}
