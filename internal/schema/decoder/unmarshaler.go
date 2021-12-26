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


//Unmarshaler represnets unmarshaler
type Unmarshaler interface {
	gojay.UnmarshalerJSONObject
	set(ptr interface{})
}

//newUnmarshaler represents a marshaler constructor
type newUnmarshaler func(ptr interface{}) Unmarshaler



var timeType = reflect.TypeOf(time.Time{})
var timePtrType = reflect.TypeOf(&time.Time{})

func baseUnmarshaler(sourceType string, targetType reflect.Type) (func(dec *gojay.Decoder, dest unsafe.Pointer) error, error) {
	switch sourceType {
	case "BIGNUMERIC", "BIGDECIMAL", "INT64", "INT", "SMALLINT", "INTEGER", "BIGINT", "TINYINT", "BYTEINT":
		switch targetType.Kind() {
		case reflect.Uint, reflect.Int, reflect.Int64, reflect.Uint64:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				i, ok, err := decodeInt(dec)
				if err != nil || !ok {
					return err
				}
				*(*int64)(dest) = int64(i)
				return nil
			}, nil
		case reflect.Int32, reflect.Uint32:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				i, ok, err := decodeInt(dec)
				if err != nil || !ok {
					return err
				}
				*(*int32)(dest) = int32(i)
				return nil
			}, nil
		case reflect.Int16, reflect.Uint16:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				i, ok, err := decodeInt(dec)
				if err != nil || !ok {
					return err
				}
				*(*int16)(dest) = int16(i)
				return nil
			}, nil
		case reflect.Int8, reflect.Uint8:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				i, ok, err := decodeInt(dec)
				if err != nil || !ok {
					return err
				}
				*(*int8)(dest) = int8(i)
				return nil
			}, nil
		case reflect.String:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) (err error) {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = text
				return err
			}, nil
		case reflect.Interface:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				i, ok, err := decodeInt(dec)
				if err != nil || !ok {
					return err
				}
				*(*interface{})(dest) = i
				return nil
			}, nil
		default:
			return nil, fmt.Errorf("unsupported binding type %v to %s", sourceType, targetType.String())
		}
	case "BYTES":
		switch targetType.Kind() {
		case reflect.Slice:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				data, err := base64.StdEncoding.DecodeString(text)
				if err != nil || !ok {
					return err
				}
				*(*[]byte)(dest) = data
				return nil
			}, nil
		case reflect.String:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = text
				return nil
			}, nil
		case reflect.Interface:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				data, err := base64.StdEncoding.DecodeString(text)
				if err != nil || !ok {
					return err
				}
				*(*interface{})(dest) = data
				return nil
			}, nil
		default:
			return nil, fmt.Errorf("unsupported binding type %v to %s", sourceType, targetType.String())
		}
	case "STRING":
		switch targetType.Kind() {
		case reflect.String:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = text
				return nil
			}, nil
		case reflect.Interface:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*interface{})(dest) = text
				return nil
			}, nil
		default:
			return nil, fmt.Errorf("unsupported binding type %v to %s", sourceType, targetType.String())
		}
	case "NUMERIC", "DECIMAL", "FLOAT64", "FLOAT":
		switch targetType.Kind() {
		case reflect.Float32:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				f, ok, err := decodeFloat(dec)
				if err != nil || !ok {
					return err
				}
				*(*float32)(dest) = float32(f)
				return nil
			}, nil
		case reflect.Float64:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				f, ok, err := decodeFloat(dec)
				if err != nil || !ok {
					return err
				}
				*(*float64)(dest) = float64(f)
				return nil
			}, nil
		case reflect.String:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = text
				return nil
			}, nil
		case reflect.Interface:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				f, ok, err := decodeFloat(dec)
				if err != nil || !ok {
					return err
				}
				*(*interface{})(dest) = f
				return nil
			}, nil
		default:
			return nil, fmt.Errorf("unsupported binding type %v to %s", sourceType, targetType.String())
		}
	case "TIME", "TIMESTAMP", "DATE", "DATETIME":
		switch targetType.Kind() {
		case reflect.Uint, reflect.Int, reflect.Int64, reflect.Uint64:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				ts, ok, err := decodeTime(dec)
				if err != nil || !ok {
					return err
				}
				*(*int64)(dest) = ts.UnixNano()
				return nil
			}, nil
		case reflect.Int32, reflect.Uint32:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				ts, ok, err := decodeTime(dec)
				if err != nil || !ok {
					return err
				}
				*(*int32)(dest) = int32(ts.Unix())
				return nil
			}, nil

		case reflect.String:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				ts, ok, err := decodeTime(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = ts.Format(time.RFC3339Nano)
				return nil
			}, nil
		case reflect.Interface:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				ts, ok, err := decodeTime(dec)
				if err != nil || !ok {
					return err
				}
				*(*interface{})(dest) = ts
				return nil
			}, nil
		case reflect.Struct:
			if targetType.ConvertibleTo(timeType) {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					ts, ok, err := decodeTime(dec)
					if err != nil || !ok {
						return err
					}
					if err != nil || !ok {
						return err
					}
					*(*time.Time)(dest) = ts
					return nil
				}, nil
			}
		case reflect.Ptr:
			if targetType.ConvertibleTo(timePtrType) {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					ts, ok, err := decodeTime(dec)
					if err != nil || !ok {
						return err
					}
					*(**time.Time)(dest) = &ts
					return nil
				}, nil
			}
		default:
			return nil, fmt.Errorf("unsupporter !! binding type %v to %s", sourceType, targetType.String())
		}
	case "BOOLEAN":
		switch targetType.Kind() {
		case reflect.Bool:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				b, ok, err := decodeBool(dec)
				if err != nil || !ok {
					return err
				}
				*(*bool)(dest) = b
				return nil
			}, nil
		case reflect.Int, reflect.Int8, reflect.Uint8, reflect.Uint:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				b, ok, err := decodeBool(dec)
				if err != nil || !ok {
					return err
				}
				v := int8(0)
				if b {
					v = 1
				}
				*(*int8)(dest) = v
				return nil
			}, nil
		case reflect.Interface:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				b, ok, err := decodeBool(dec)
				if err != nil || !ok {
					return err
				}
				*(*interface{})(dest) = b
				return nil
			}, nil
		case reflect.String:
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				b, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = b
				return nil
			}, nil
		default:
			return nil, fmt.Errorf("unsupporter binding type %v to %s", sourceType, targetType.String())
		}
	}
	return nil, fmt.Errorf("unsupporter binding type %v to %s", sourceType, targetType.String())
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

func decodeString(dec *gojay.Decoder) (string, bool, error) {
	var value *string
	err := dec.StringNull(&value)
	if err != nil || value == nil {
		return "", false, err
	}
	return *value, true, nil
}
