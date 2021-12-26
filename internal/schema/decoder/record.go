package decoder

import (
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/xunsafe"
	"google.golang.org/api/bigquery/v2"
	"reflect"
	"unsafe"
)

//record represents record unmarshaler
type record struct {
	ptr             unsafe.Pointer
	fields          []*xunsafe.Field
	unmarshaler     []Unmarshaler
	newUnmarshalers []newUnmarshaler
	index           int
}

//UnmarshalJSONArray unmarshal JSON array
func (o *record) UnmarshalJSONArray(dec *gojay.Decoder) error {
	if o.index >= len(o.fields) {
		o.index = 0
	}
	i := o.index
	field := o.fields[i]
	ptr := field.Addr(o.ptr)
	if o.unmarshaler[i] == nil {
		o.unmarshaler[i] = o.newUnmarshalers[i](ptr)
	} else {
		o.unmarshaler[i].set(ptr)
	}
	o.index++
	return dec.Object(o.unmarshaler[i])
}

func (o *record) set(ptr interface{}) {
	o.ptr = xunsafe.AsPointer(ptr)
}

//UnmarshalJSONObject unmarshal JSON object
func (o *record) UnmarshalJSONObject(dec *gojay.Decoder, key string) error {
	switch key[0] {
	case 'v':
		return dec.Object(o)
	case 'f':
		return dec.Array(o)
	}
	return fmt.Errorf("unsupported key :%v", key)
}

//NKeys returns max of expected keys
func (o *record) NKeys() int {
	return 1
}

func newRecordUnmarshaler(field *bigquery.TableFieldSchema, dest reflect.Type) (func(ptr interface{}) Unmarshaler, error) {
	machedFields, err := matchFields(dest, field)
	if err != nil {
		return nil, err
	}
	var fields = make([]*xunsafe.Field, len(field.Fields))
	var newUnmarshalers = make([]newUnmarshaler, len(field.Fields))
	for i := range machedFields {
		fieldUnmarsher, field, err := newFieldUnmarshaler(machedFields[i], field.Fields[i])
		if err != nil {
			return nil, err
		}
		newUnmarshalers[i] = fieldUnmarsher
		fields[i] = field
	}
	return func(ptr interface{}) Unmarshaler {
		result := &record{
			ptr:             xunsafe.AsPointer(ptr),
			fields:          fields,
			newUnmarshalers: newUnmarshalers,
			unmarshaler:     make([]Unmarshaler, len(fields)),
		}
		return result
	}, nil
}

func newFieldUnmarshaler(field *reflect.StructField, schemaField *bigquery.TableFieldSchema) (newUnmarshaler, *xunsafe.Field, error) {
	fieldType := field.Type
	xField := xunsafe.NewField(*field)
	if fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
		fieldUnmarshaler, err := newJSONUnmarshaler(schemaField, fieldType)
		if err != nil {
			return nil, nil, err
		}
		return fieldUnmarshaler, xField, nil
	}
	fieldUnmarshaler, err := newJSONUnmarshaler(schemaField, fieldType)
	if err != nil {
		return nil, nil, err
	}
	return fieldUnmarshaler, xField, nil
}
