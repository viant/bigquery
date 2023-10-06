package schema

import (
	"fmt"
	"math/big"
	"reflect"
	"time"
)

// FieldType is the type of field.
type FieldType string

const (
	// FieldTypeString is a string field type.
	FieldTypeString FieldType = "STRING"
	// FieldTypeBytes is a bytes field type.
	FieldTypeBytes FieldType = "BYTES"
	// FieldTypeInteger is a integer field type.
	FieldTypeInteger FieldType = "INTEGER"
	// FieldTypeFloat is a float field type.
	FieldTypeFloat FieldType = "FLOAT"
	// FieldTypeBoolean is a boolean field type.
	FieldTypeBoolean FieldType = "BOOLEAN"
	// FieldTypeBool is a bool field type.
	FieldTypeBool FieldType = "BOOL"

	// FieldTypeTimestamp is a timestamp field type.
	FieldTypeTimestamp FieldType = "TIMESTAMP"
	// FieldTypeRecord is a record field type.
	FieldTypeRecord FieldType = "RECORD"
	// FieldTypeDate is a date field type.
	FieldTypeDate FieldType = "DATE"
	// FieldTypeTime is a time field type.
	FieldTypeTime FieldType = "TIME"
	// FieldTypeDateTime is a datetime field type.
	FieldTypeDateTime FieldType = "DATETIME"
	// FieldTypeNumeric is a numeric field type.
	FieldTypeNumeric FieldType = "NUMERIC"
	// FieldTypeBigNumeric is a numeric field type that supports values of larger precision
	FieldTypeBigNumeric FieldType = "BIGNUMERIC"
)

var (
	intType     = reflect.TypeOf(int(0))
	float64Type = reflect.TypeOf(float64(0))
	bytesType   = reflect.TypeOf([]byte{})
	stringType  = reflect.TypeOf("")
	timeType    = reflect.TypeOf(time.Time{})
	timeTypePtr = reflect.TypeOf(&time.Time{})
	boolType    = reflect.TypeOf(false)
	ratType     = reflect.TypeOf(big.Rat{})
)

func mapBasicType(dataType string, nullable bool) (reflect.Type, error) {
	rType, err := mapBasicRawType(dataType)
	if err != nil {
		return nil, err
	}
	if nullable {
		rType = reflect.PtrTo(rType)
	}
	return rType, nil
}

func mapBasicRawType(dataType string) (reflect.Type, error) {
	switch FieldType(dataType) {
	case FieldTypeInteger:
		return intType, nil
	case FieldTypeBytes:
		return bytesType, nil
	case FieldTypeString:
		return stringType, nil
	case FieldTypeNumeric, FieldTypeFloat:
		return float64Type, nil
	case FieldTypeTime, FieldTypeTimestamp, FieldTypeDate, FieldTypeDateTime:
		return timeType, nil
	case FieldTypeBoolean, FieldTypeBool:
		return boolType, nil
	case FieldTypeBigNumeric:
		return ratType, nil
	default:
		return nil, fmt.Errorf("unsupported type: %v", dataType)
	}
}
