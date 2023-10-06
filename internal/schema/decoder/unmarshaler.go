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
}

// newUnmarshaler represents a marshaler constructor
type newUnmarshaler func(ptr interface{}) Unmarshaler

var timeType = reflect.TypeOf(time.Time{})
var timePtrType = reflect.TypeOf(&time.Time{})

func baseUnmarshaler(sourceType string, targetType reflect.Type) (func(dec *gojay.Decoder, dest unsafe.Pointer) error, error) {
	isPtr := targetType.Kind() == reflect.Ptr
	if isPtr {
		targetType = targetType.Elem()
	}
	switch sourceType {
	case "BIGNUMERIC", "BIGDECIMAL", "INT64", "INT", "SMALLINT", "INTEGER", "BIGINT", "TINYINT", "BYTEINT":
		switch targetType.Kind() {
		case reflect.Uint, reflect.Int, reflect.Int64, reflect.Uint64:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					i, ok, err := decodeInt(dec)
					if err != nil || !ok {
						return err
					}
					v := int64(i)
					*(**int64)(dest) = &v
					return nil
				}, nil
			}

			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				i, ok, err := decodeInt(dec)
				if err != nil || !ok {
					return err
				}
				*(*int64)(dest) = int64(i)
				return nil
			}, nil
		case reflect.Int32, reflect.Uint32:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					i, ok, err := decodeInt(dec)
					if err != nil || !ok {
						return err
					}
					v := int32(i)
					*(**int32)(dest) = &v
					return nil
				}, nil
			}
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				i, ok, err := decodeInt(dec)
				if err != nil || !ok {
					return err
				}
				*(*int32)(dest) = int32(i)
				return nil
			}, nil
		case reflect.Int16, reflect.Uint16:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					i, ok, err := decodeInt(dec)
					if err != nil || !ok {
						return err
					}
					v := int16(i)
					*(**int16)(dest) = &v
					return nil
				}, nil
			}
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				i, ok, err := decodeInt(dec)
				if err != nil || !ok {
					return err
				}
				*(*int16)(dest) = int16(i)
				return nil
			}, nil
		case reflect.Int8, reflect.Uint8:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					i, ok, err := decodeInt(dec)
					if err != nil || !ok {
						return err
					}
					v := int8(i)
					*(**int8)(dest) = &v
					return nil
				}, nil
			}
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				i, ok, err := decodeInt(dec)
				if err != nil || !ok {
					return err
				}
				*(*int8)(dest) = int8(i)
				return nil
			}, nil
		case reflect.String:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) (err error) {
					text, ok, err := decodeString(dec)
					if err != nil || !ok {
						return err
					}

					*(**string)(dest) = &text
					return err
				}, nil
			}
			return func(dec *gojay.Decoder, dest unsafe.Pointer) (err error) {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = text
				return err
			}, nil
		case reflect.Interface:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					i, ok, err := decodeInt(dec)
					if err != nil || !ok {
						return err
					}
					var v interface{} = i
					*(**interface{})(dest) = &v
					return nil
				}, nil
			}

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
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					text, ok, err := decodeString(dec)
					if err != nil || !ok {
						return err
					}
					*(**string)(dest) = &text
					return nil
				}, nil
			}
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = text
				return nil
			}, nil
		case reflect.Interface:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					text, ok, err := decodeString(dec)
					if err != nil || !ok {
						return err
					}
					data, err := base64.StdEncoding.DecodeString(text)
					if err != nil || !ok {
						return err
					}
					var v interface{} = data
					*(**interface{})(dest) = &v
					return nil
				}, nil
			}

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
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					text, ok, err := decodeString(dec)
					if err != nil || !ok {
						return err
					}
					*(**string)(dest) = &text
					return nil
				}, nil
			}
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = text
				return nil
			}, nil
		case reflect.Interface:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					text, ok, err := decodeString(dec)
					if err != nil || !ok {
						return err
					}
					var v interface{} = text
					*(**interface{})(dest) = &v
					return nil
				}, nil
			}

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
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					f, ok, err := decodeFloat(dec)
					if err != nil || !ok {
						return err
					}
					v := float32(f)
					*(**float32)(dest) = &v
					return nil
				}, nil
			}
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				f, ok, err := decodeFloat(dec)
				if err != nil || !ok {
					return err
				}
				*(*float32)(dest) = float32(f)
				return nil
			}, nil
		case reflect.Float64:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					f, ok, err := decodeFloat(dec)
					if err != nil || !ok {
						return err
					}
					v := float64(f)
					*(**float64)(dest) = &v
					return nil
				}, nil
			}
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				f, ok, err := decodeFloat(dec)
				if err != nil || !ok {
					return err
				}
				*(*float64)(dest) = float64(f)
				return nil
			}, nil
		case reflect.String:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					text, ok, err := decodeString(dec)
					if err != nil || !ok {
						return err
					}
					*(**string)(dest) = &text
					return nil
				}, nil
			}
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				text, ok, err := decodeString(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = text
				return nil
			}, nil
		case reflect.Interface:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					f, ok, err := decodeFloat(dec)
					if err != nil || !ok {
						return err
					}
					var v interface{} = f
					*(**interface{})(dest) = &v
					return nil
				}, nil
			}
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
	case "DATE":
		switch targetType.Kind() {
		case reflect.String:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					val := ""
					if err := dec.String(&val); err != nil {
						return err
					}
					*(**string)(dest) = &val
					return nil
				}, nil
			}

			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				val := ""
				if err := dec.String(&val); err != nil {
					return err
				}
				*(*string)(dest) = val
				return nil
			}, nil
		case reflect.Interface:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					ts, ok, err := decodeDate(dec)
					if err != nil || !ok {
						return err
					}
					var v interface{} = ts
					*(**interface{})(dest) = &v
					return nil
				}, nil
			}
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				ts, ok, err := decodeDate(dec)
				if err != nil || !ok {
					return err
				}
				*(*interface{})(dest) = ts
				return nil
			}, nil
		case reflect.Struct:
			if targetType.ConvertibleTo(timeType) {
				if isPtr {
					return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
						ts, ok, err := decodeDate(dec)
						if err != nil || !ok {
							return err
						}
						*(**time.Time)(dest) = &ts
						return nil
					}, nil
				}
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					ts, ok, err := decodeDate(dec)
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
		default:
			return nil, fmt.Errorf("unsupporter !! binding type %v to %s", sourceType, targetType.String())
		}

	case "TIME", "TIMESTAMP", "DATETIME":
		switch targetType.Kind() {

		case reflect.Uint, reflect.Int, reflect.Int64, reflect.Uint64:

			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					ts, ok, err := decodeTime(dec)
					if err != nil || !ok {
						return err
					}
					t := ts.UnixNano()
					*(**int64)(dest) = &t
					return nil
				}, nil
			}
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				ts, ok, err := decodeTime(dec)
				if err != nil || !ok {
					return err
				}
				*(*int64)(dest) = ts.UnixNano()
				return nil
			}, nil
		case reflect.Int32, reflect.Uint32:

			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					ts, ok, err := decodeTime(dec)
					if err != nil || !ok {
						return err
					}
					v := int32(ts.Unix())
					*(**int32)(dest) = &v
					return nil
				}, nil
			}

			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				ts, ok, err := decodeTime(dec)
				if err != nil || !ok {
					return err
				}
				*(*int32)(dest) = int32(ts.Unix())
				return nil
			}, nil

		case reflect.String:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					ts, ok, err := decodeTime(dec)
					if err != nil || !ok {
						return err
					}
					t := ts.Format(time.RFC3339Nano)
					*(**string)(dest) = &t
					return nil
				}, nil
			}
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				ts, ok, err := decodeTime(dec)
				if err != nil || !ok {
					return err
				}
				*(*string)(dest) = ts.Format(time.RFC3339Nano)
				return nil
			}, nil
		case reflect.Interface:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					ts, ok, err := decodeTime(dec)
					if err != nil || !ok {
						return err
					}
					var v interface{} = ts
					*(**interface{})(dest) = &v
					return nil
				}, nil
			}
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
				if isPtr {
					return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
						ts, ok, err := decodeTime(dec)
						if err != nil || !ok {
							return err
						}
						*(**time.Time)(dest) = &ts
						return nil
					}, nil
				}
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
		default:
			return nil, fmt.Errorf("unsupporter !! binding type %v to %s", sourceType, targetType.String())
		}
	case "BOOLEAN":
		switch targetType.Kind() {
		case reflect.Bool:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					b, ok, err := decodeBool(dec)
					if err != nil || !ok {
						return err
					}
					*(**bool)(dest) = &b
					return nil
				}, nil

			}
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				b, ok, err := decodeBool(dec)
				if err != nil || !ok {
					return err
				}
				*(*bool)(dest) = b
				return nil
			}, nil
		case reflect.Int, reflect.Int8, reflect.Uint8, reflect.Uint:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					b, ok, err := decodeBool(dec)
					if err != nil || !ok {
						return err
					}
					v := int8(0)
					if b {
						v = 1
					}
					*(**int8)(dest) = &v
					return nil
				}, nil
			}
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
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					b, ok, err := decodeBool(dec)
					if err != nil || !ok {
						return err
					}
					var v interface{} = b
					*(**interface{})(dest) = &v
					return nil
				}, nil
			}
			return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
				b, ok, err := decodeBool(dec)
				if err != nil || !ok {
					return err
				}
				*(*interface{})(dest) = b
				return nil
			}, nil
		case reflect.String:
			if isPtr {
				return func(dec *gojay.Decoder, dest unsafe.Pointer) error {
					b, ok, err := decodeString(dec)
					if err != nil || !ok {
						return err
					}
					*(**string)(dest) = &b
					return nil
				}, nil
			}
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
