package internal

import (
	"github.com/viant/bigquery/internal/schema"
	"github.com/viant/bigquery/internal/schema/decoder"
	"github.com/viant/xunsafe"
	"google.golang.org/api/bigquery/v2"
	"reflect"
)

// Session represents decoding session
type Session struct {
	Rows          []Region
	Data          []byte
	Decoder       *decoder.Decoder
	Pointers      []interface{}
	ValuePointers []reflect.Value
	DestTypes     []reflect.Type
	XTypes        []xunsafe.Type
	Columns       []string
	Schema        *bigquery.TableSchema
	TotalRows     uint64
}

// Init initialises session
func (s *Session) Init(tableSchema *bigquery.TableSchema) error {
	var err error
	if s.DestTypes, err = schema.BuildSchemaTypes(tableSchema); err != nil {
		return err
	}
	newDecoder, err := decoder.New(s.DestTypes, tableSchema)
	if err != nil {
		return err
	}
	s.XTypes = make([]xunsafe.Type, len(s.DestTypes))
	s.ValuePointers = make([]reflect.Value, len(s.DestTypes))
	s.Columns = make([]string, len(s.DestTypes))
	s.Pointers = make([]interface{}, len(s.DestTypes))
	for i, t := range s.DestTypes {
		s.ValuePointers[i] = reflect.New(t)
		s.Pointers[i] = s.ValuePointers[i].Interface()
		s.XTypes[i] = *xunsafe.NewType(s.DestTypes[i])
		s.Columns[i] = tableSchema.Fields[i].Name
	}
	s.Decoder = newDecoder(s.Pointers)
	return nil
}

func (s *Session) Reset() {
	for i, t := range s.DestTypes {
		rType := s.ValuePointers[i].Type()
		if rType.Kind() == reflect.Ptr {
			rType = rType.Elem()
		}
		switch rType.Kind() { //reinitialize complex types
		case reflect.Slice, reflect.Struct:
			s.ValuePointers[i] = reflect.New(t)
			s.Pointers[i] = s.ValuePointers[i].Interface()
		}
	}
}

// Region represents a data region
type Region struct {
	Begin int
	End   int
}
