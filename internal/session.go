package internal

import (
	"github.com/viant/bigquery/internal/schema"
	"github.com/viant/bigquery/internal/schema/decoder"
	"github.com/viant/xunsafe"
	"google.golang.org/api/bigquery/v2"
	"reflect"
)

//Session represents decoding session
type Session struct {
	Rows      []Region
	Data      []byte
	Decoder   *decoder.Decoder
	Pointers  []interface{}
	DestTypes []reflect.Type
	XTypes    []xunsafe.Type
	Columns   []string
	Schema    *bigquery.TableSchema
	TotalRows uint64
}

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
	s.Columns = make([]string, len(s.DestTypes))
	s.Pointers = make([]interface{}, len(s.DestTypes))
	for i, t := range s.DestTypes {
		s.Pointers[i] = reflect.New(t).Interface()
		s.XTypes[i] = *xunsafe.NewType(s.DestTypes[i])
		s.Columns[i] = tableSchema.Fields[i].Name
	}
	s.Decoder = newDecoder(s.Pointers)
	return nil
}

type Region struct {
	Begin int
	End   int
}
