package decoder

import (
	"fmt"
	"github.com/francoispqt/gojay"
	"google.golang.org/api/bigquery/v2"
	"reflect"
)

// Decoder represents value decoder
type Decoder struct {
	values          []interface{}
	newUnmarshalers []newUnmarshaler
	unmarshalers    []Unmarshaler
}

// SetValues sets values pointer
func (d *Decoder) SetValues(values []interface{}) {
	d.values = values
}

// UnmarshalJSONObject unmarshal json object
func (d *Decoder) UnmarshalJSONObject(dec *gojay.Decoder, _ string) error {
	return dec.Array(d)
}

// NKeys returns expcted max keys for the decoder
func (d *Decoder) NKeys() int {
	return 1
}

// Set sets value
func (d *Decoder) Set(values []interface{}) {
	d.values = values
}

// UnmarshalJSONArray unmarshal JSON array
func (d *Decoder) UnmarshalJSONArray(dec *gojay.Decoder) error {
	for i := range d.values {

		ptr := d.values[i]
		if d.unmarshalers[i] == nil {
			d.unmarshalers[i] = d.newUnmarshalers[i](ptr)
		} else {

			d.unmarshalers[i].set(ptr)
		}
		unmarhsaler := d.unmarshalers[i]
		if err := dec.Object(unmarhsaler); err != nil {
			return err
		}
	}
	return nil
}

// New creates a new decoder
func New(types []reflect.Type, schema *bigquery.TableSchema) (func(values []interface{}) *Decoder, error) {
	var newUnmarshalersFn []newUnmarshaler
	for i := range types {
		unMarshaler, err := newJSONUnmarshaler(schema.Fields[i], types[i])
		if err != nil {
			return nil, err
		}
		newUnmarshalersFn = append(newUnmarshalersFn, unMarshaler)
	}
	return func(values []interface{}) *Decoder {
		return &Decoder{
			values:          values,
			newUnmarshalers: newUnmarshalersFn,
			unmarshalers:    make([]Unmarshaler, len(types)),
		}
	}, nil
}

func newJSONUnmarshaler(field *bigquery.TableFieldSchema, dest reflect.Type) (newUnmarshaler, error) {
	switch field.Mode {
	case "REPEATED":
		return newValueSliceUnmarshaler(field, dest)
	case "NULLABLE":
		if field.Type == "RECORD" {
			return newRecordUnmarshaler(field, dest)
		}
		return newValueUnmarshaler(field, dest)
	}
	return nil, fmt.Errorf("unsupported %v %s", dest.String(), field.Type)
}
