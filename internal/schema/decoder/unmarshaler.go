package decoder

import (
	"encoding/base64"
	"fmt"
	"github.com/francoispqt/gojay"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

// Unmarshaler represnets unmarshaler
type Unmarshaler interface {
	gojay.UnmarshalerJSONObject
	set(ptr interface{})
	value() interface{}
}

// newUnmarshaler represents a marshaler constructor
type newUnmarshaler func(ptr interface{}) Unmarshaler

var timeType = reflect.TypeOf(time.Time{})

// decodeValue decode JSON value
func decodeValue[T any](isPointer bool, decode func(dec *gojay.Decoder) (T, bool, error)) func(dec *gojay.Decoder, dest unsafe.Pointer) error {
	if isPointer {
		return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
			b, ok, err := decode(dec)
			if err != nil || !ok {
				*(**T)(dest) = nil
				return err
			}
			*(**T)(dest) = &b
			return nil
		}
	}
	return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
		b, ok, err := decode(dec)
		if err != nil || !ok {
			return err
		}
		*(*T)(dest) = b
		return nil
	}
}

func baseUnmarshaler(sourceType string, targetType reflect.Type) (func(dec *gojay.Decoder, dest unsafe.Pointer) error, error) {
	isPtr := targetType.Kind() == reflect.Ptr
	if isPtr {
		targetType = targetType.Elem()
	}
	switch sourceType {
	case "BIGNUMERIC", "BIGDECIMAL", "INT64", "INT", "SMALLINT", "INTEGER", "BIGINT", "TINYINT", "BYTEINT":
		switch targetType.Kind() {
		case reflect.Uint, reflect.Int, reflect.Int64, reflect.Uint64:
			return decodeValue[int](isPtr, decodeInt), nil
		case reflect.Int32, reflect.Uint32:
			return decodeValue[int32](isPtr, decodeInt32), nil
		case reflect.Int16, reflect.Uint16:
			return decodeValue[int16](isPtr, decodeInt16), nil
		case reflect.Int8, reflect.Uint8:
			return decodeValue[int8](isPtr, decodeInt8), nil
		case reflect.String:
			return decodeValue[string](isPtr, decodeString), nil
		case reflect.Interface:
			return decodeValue[interface{}](isPtr, decodeIntInterface), nil
		default:
			return nil, fmt.Errorf("unsupported binding type %v to %s", sourceType, targetType.String())
		}
	case "BYTES":
		switch targetType.Kind() {
		case reflect.Slice:
			return decodeValue[[]byte](isPtr, decodeBytes), nil
		case reflect.String:
			return decodeValue[string](isPtr, decodeString), nil
		case reflect.Interface:
			return decodeValue[interface{}](isPtr, decodeStringInterface), nil
		default:
			return nil, fmt.Errorf("unsupported binding type %v to %s", sourceType, targetType.String())
		}
	case "STRING":
		switch targetType.Kind() {
		case reflect.String:
			return decodeValue[string](isPtr, decodeString), nil
		case reflect.Interface:
			return decodeValue[interface{}](isPtr, decodeStringInterface), nil
		default:
			return nil, fmt.Errorf("unsupported binding type %v to %s", sourceType, targetType.String())
		}
	case "NUMERIC", "DECIMAL", "FLOAT64", "FLOAT":
		switch targetType.Kind() {
		case reflect.Float32:
			return decodeValue[float32](isPtr, decodeFloat32), nil
		case reflect.Float64:
			return decodeValue[float64](isPtr, decodeFloat), nil
		case reflect.String:
			return decodeValue[string](isPtr, decodeString), nil
		case reflect.Interface:
			return decodeValue[interface{}](isPtr, decodeFloatInterface), nil
		default:
			return nil, fmt.Errorf("unsupported binding type %v to %s", sourceType, targetType.String())
		}
	case "DATE":
		switch targetType.Kind() {
		case reflect.String:
			return decodeValue[string](isPtr, decodeString), nil
		case reflect.Interface:
			return decodeValue[interface{}](isPtr, decodeDateInterface), nil
		case reflect.Struct:
			if targetType.ConvertibleTo(timeType) {
				return decodeValue[time.Time](isPtr, decodeDate), nil
			}
			fallthrough
		default:
			return nil, fmt.Errorf("unsupporter !! binding type %v to %s", sourceType, targetType.String())
		}
	case "TIME", "TIMESTAMP", "DATETIME":
		switch targetType.Kind() {
		case reflect.Uint, reflect.Int, reflect.Int64, reflect.Uint64:
			return decodeValue[int64](isPtr, decodeTimeUnixNano), nil
		case reflect.Int32, reflect.Uint32:
			return decodeValue[int32](isPtr, decodeTimeUnix), nil
		case reflect.String:
			return decodeValue[string](isPtr, decodeTimeString), nil
		case reflect.Interface:
			return decodeValue[interface{}](isPtr, decodeTimeInterface), nil
		case reflect.Struct:
			if targetType.ConvertibleTo(timeType) {
				return decodeValue[time.Time](isPtr, decodeTime), nil
			}
			fallthrough
		default:
			return nil, fmt.Errorf("unsupporter !! binding type %v to %s", sourceType, targetType.String())
		}
	case "BOOLEAN":
		switch targetType.Kind() {
		case reflect.Bool:
			return decodeValue[bool](isPtr, decodeBool), nil
		case reflect.Int8, reflect.Uint8:
			return decodeValue[int8](isPtr, decodeInt8Bool), nil
		case reflect.Int, reflect.Int64, reflect.Uint64, reflect.Uint:
			return decodeValue[int](isPtr, decodeIntBool), nil
		case reflect.Interface:
			return decodeValue[interface{}](isPtr, decodeBoolInterface), nil
		case reflect.String:
			return decodeValue[string](isPtr, decodeString), nil
		default:
			return nil, fmt.Errorf("unsupporter binding type %v to %s", sourceType, targetType.String())
		}
	}
	return nil, fmt.Errorf("unsupporter binding type %v to %s", sourceType, targetType.String())
}

func decodeBytes(dec *gojay.Decoder) ([]byte, bool, error) {
	text, ok, err := decodeString(dec)
	if err != nil || !ok {
		return nil, false, err
	}
	data, err := base64.StdEncoding.DecodeString(text)
	return data, err == nil, err
}

func decodeTime(dec *gojay.Decoder) (time.Time, bool, error) {
	f, ok, err := decodeFloat(dec)
	if err != nil || !ok {
		return time.Time{}, false, err
	}
	timestamp := int64(f*1000000) * int64(time.Microsecond)
	ts := time.Unix(0, timestamp)
	return ts, true, nil
}

func decodeTimeString(dec *gojay.Decoder) (string, bool, error) {
	ts, ok, err := decodeTime(dec)
	if err != nil || !ok {
		return "", false, err
	}
	t := ts.Format(time.RFC3339Nano)
	return t, true, nil
}

func decodeTimeUnixNano(dec *gojay.Decoder) (int64, bool, error) {
	ts, ok, err := decodeTime(dec)
	if err != nil || !ok {
		return 0, false, err
	}
	t := ts.UnixNano()
	return t, true, nil
}

func decodeTimeUnix(dec *gojay.Decoder) (int32, bool, error) {
	ts, ok, err := decodeTime(dec)
	if err != nil || !ok {
		return 0, false, err
	}
	t := ts.Unix()
	return int32(t), true, nil
}

func decodeDate(dec *gojay.Decoder) (time.Time, bool, error) {
	v := ""
	if err := dec.String(&v); err != nil {
		return time.Time{}, false, err
	}
	if v == "" {
		return time.Time{}, true, nil
	}
	t, err := time.Parse("2006-01-02", v)
	return t, true, err
}

func decodeInt(dec *gojay.Decoder) (int, bool, error) {
	var value *string
	err := dec.StringNull(&value)
	if err != nil || value == nil {
		return 0, false, err
	}
	i, err := strconv.Atoi(*value)
	if err != nil {
		return 0, false, err
	}
	return i, true, nil
}

func decodeBoolInterface(dec *gojay.Decoder) (interface{}, bool, error) {
	v, ok, err := decodeBool(dec)
	if err != nil || !ok {
		return nil, false, err
	}
	return v, true, nil
}

func decodeIntInterface(dec *gojay.Decoder) (interface{}, bool, error) {
	v, ok, err := decodeInt(dec)
	if err != nil || !ok {
		return nil, false, err
	}
	return v, true, nil
}

func decodeStringInterface(dec *gojay.Decoder) (interface{}, bool, error) {
	v, ok, err := decodeString(dec)
	if err != nil || !ok {
		return nil, false, err
	}
	return v, true, nil
}

func decodeDateInterface(dec *gojay.Decoder) (interface{}, bool, error) {
	v, ok, err := decodeDate(dec)
	if err != nil || !ok {
		return nil, false, err
	}
	return v, true, nil
}

func decodeTimeInterface(dec *gojay.Decoder) (interface{}, bool, error) {
	v, ok, err := decodeTime(dec)
	if err != nil || !ok {
		return nil, false, err
	}
	return v, true, nil
}

func decodeFloatInterface(dec *gojay.Decoder) (interface{}, bool, error) {
	v, ok, err := decodeFloat(dec)
	if err != nil || !ok {
		return nil, false, err
	}
	return v, true, nil
}
func decodeInt32(dec *gojay.Decoder) (int32, bool, error) {
	i, ok, err := decodeInt(dec)
	if err != nil || !ok {
		return 0, false, err
	}
	return int32(i), true, nil
}

func decodeInt16(dec *gojay.Decoder) (int16, bool, error) {
	i, ok, err := decodeInt(dec)
	if err != nil || !ok {
		return 0, false, err
	}
	return int16(i), true, nil
}

func decodeInt8(dec *gojay.Decoder) (int8, bool, error) {
	i, ok, err := decodeInt(dec)
	if err != nil || !ok {
		return 0, false, err
	}
	return int8(i), true, nil
}

func decodeBool(dec *gojay.Decoder) (bool, bool, error) {
	var value *string
	err := dec.StringNull(&value)
	if err != nil || value == nil {
		return false, false, err
	}
	b, err := strconv.ParseBool(*value)
	if err != nil {
		return false, false, err
	}
	return b, true, nil
}

func decodeInt8Bool(dec *gojay.Decoder) (int8, bool, error) {
	var value *string
	err := dec.StringNull(&value)
	if err != nil || value == nil {
		return 9, false, err
	}
	b, err := strconv.ParseBool(*value)
	if err != nil || !b {
		return 0, false, err
	}
	return 1, true, nil
}

func decodeIntBool(dec *gojay.Decoder) (int, bool, error) {
	var value *string
	err := dec.StringNull(&value)
	if err != nil || value == nil {
		return 9, false, err
	}
	b, err := strconv.ParseBool(*value)
	if err != nil || !b {
		return 0, false, err
	}
	return 1, true, nil
}

func decodeFloat(dec *gojay.Decoder) (float64, bool, error) {
	var value *string
	err := dec.StringNull(&value)
	if err != nil || value == nil {
		return 0, false, err
	}
	if value == nil {
		return 0, false, nil
	}
	i, err := strconv.ParseFloat(*value, 64)
	if err != nil {
		return 0, false, err
	}
	return i, true, nil

}

func decodeFloat32(dec *gojay.Decoder) (float32, bool, error) {
	f, ok, err := decodeFloat(dec)
	if err != nil || !ok {
		return 0, false, err
	}
	return float32(f), true, nil
}

func decodeString(dec *gojay.Decoder) (string, bool, error) {
	var value *string
	err := dec.StringNull(&value)
	if err != nil || value == nil {
		return "", false, err
	}
	return *value, true, nil
}
