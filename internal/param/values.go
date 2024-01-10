package param

import (
	"encoding/base64"
	"fmt"
	"google.golang.org/api/bigquery/v2"
	"math/big"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

var (
	valuesSpan      = reflect.UnsafePointer
	sliceIndexBegin = valuesSpan + 1
	ptrIndexBegin   = 2*valuesSpan + 1
)
var values = make([]func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error), 3*(valuesSpan+1))

func unsafeAdd(structAddr unsafe.Pointer, offset uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(structAddr) + offset)
}

func init() {

	values[reflect.Bool] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*bool)(unsafeAdd(structAddr, field.Offset))
		return NewBoolQueryParameter(field.Name, v)
	}
	values[reflect.Int] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*int)(unsafeAdd(structAddr, field.Offset))
		return NewIntQueryParameter(field.Name, v)
	}
	values[reflect.Int8] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*int8)(unsafeAdd(structAddr, field.Offset))
		return NewIntQueryParameter(field.Name, int(v))
	}
	values[reflect.Int16] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*int16)(unsafeAdd(structAddr, field.Offset))
		return NewIntQueryParameter(field.Name, int(v))
	}
	values[reflect.Int32] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*int32)(unsafeAdd(structAddr, field.Offset))
		return NewIntQueryParameter(field.Name, int(v))
	}
	values[reflect.Int64] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*int64)(unsafeAdd(structAddr, field.Offset))
		return NewIntQueryParameter(field.Name, int(v))
	}

	values[reflect.Uint] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*uint)(unsafeAdd(structAddr, field.Offset))
		return NewIntQueryParameter(field.Name, int(v))
	}
	values[reflect.Uint8] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*uint8)(unsafeAdd(structAddr, field.Offset))
		return NewIntQueryParameter(field.Name, int(v))
	}
	values[reflect.Uint16] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*uint16)(unsafeAdd(structAddr, field.Offset))
		return NewIntQueryParameter(field.Name, int(v))
	}
	values[reflect.Uint32] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*uint32)(unsafeAdd(structAddr, field.Offset))
		return NewIntQueryParameter(field.Name, int(v))
	}
	values[reflect.Uint64] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*uint64)(unsafeAdd(structAddr, field.Offset))
		return NewIntQueryParameter(field.Name, int(v))
	}

	values[reflect.Float32] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*float32)(unsafeAdd(structAddr, field.Offset))
		return NewFloatQueryParameter(field.Name, float64(v))
	}
	values[reflect.Float64] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*float64)(unsafeAdd(structAddr, field.Offset))
		return NewFloatQueryParameter(field.Name, v)
	}

	values[reflect.String] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*string)(unsafeAdd(structAddr, field.Offset))
		return NewStringQueryParameter(field.Name, v)
	}

	values[reflect.Slice] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		item := field.Type.Elem()
		return values[sliceIndexBegin+item.Kind()](field, structAddr)
	}

	values[reflect.Ptr] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		item := field.Type
		if field.Offset == 0 && item.Kind() == reflect.Ptr {
			structAddr = *(*unsafe.Pointer)(structAddr)
			item = item.Elem()
			field.Type = item
		}
		return values[ptrIndexBegin+item.Kind()](field, structAddr)
	}

	values[reflect.Struct] = func(owner reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		ptr := unsafeAdd(structAddr, owner.Offset)
		ownerType := owner.Type
		switch ownerType {
		case reflect.TypeOf(time.Time{}):
			v := (*time.Time)(ptr)
			return NewTimeQueryParameter(owner.Name, v)
		case reflect.TypeOf(big.Rat{}):
			v := (*big.Rat)(ptr)
			return NewBigNumericQueryParameter(owner.Name, v)
		}

		var structValues = make(map[string]bigquery.QueryParameterValue)
		var structTypes = make([]*bigquery.QueryParameterTypeStructTypes, 0)
		for i := 0; i < ownerType.NumField(); i++ {
			field := owner.Type.Field(i)
			key := valueKey(field.Type)
			fn := values[valueKey(field.Type)]
			if fn == nil {
				return nil, fmt.Errorf("unsupported %v, %v", field.Type.String(), key)
			}
			param, err := fn(field, ptr)
			if err != nil {
				return nil, err
			}
			structValues[field.Name] = *param.ParameterValue
			structTypes = append(structTypes, &bigquery.QueryParameterTypeStructTypes{
				Name: field.Name,
				Type: param.ParameterType,
			})
		}
		return &bigquery.QueryParameter{
			Name:           owner.Name,
			ParameterType:  &bigquery.QueryParameterType{StructTypes: structTypes},
			ParameterValue: &bigquery.QueryParameterValue{StructValues: structValues},
		}, nil
	}

	values[ptrIndexBegin+reflect.Struct] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		ptr := unsafeAdd(structAddr, field.Offset)

		ownerType := field.Type
		switch ownerType {
		case reflect.TypeOf(time.Time{}):
			v := (*time.Time)(ptr)
			if v == nil {
				return NewTimeQueryParameter(field.Name, nil)
			}
			return NewTimeQueryParameter(field.Name, v)
		case reflect.TypeOf(big.Rat{}):
			v := (*big.Rat)(ptr)
			if v == nil {
				return NewBigNumericQueryParameter(field.Name, nil)
			}
			return NewBigNumericQueryParameter(field.Name, v)
		}

		if ptr == nil {
			return &bigquery.QueryParameter{
				Name:           field.Name,
				ParameterType:  &bigquery.QueryParameterType{StructTypes: []*bigquery.QueryParameterTypeStructTypes{}},
				ParameterValue: &bigquery.QueryParameterValue{StructValues: map[string]bigquery.QueryParameterValue{}},
			}, nil
		}
		structPtr := *(*unsafe.Pointer)(ptr)
		return values[reflect.Struct](reflect.StructField{
			Name: field.Name,
			Type: field.Type.Elem(),
		}, structPtr)
	}

	values[sliceIndexBegin+reflect.Bool] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*[]bool)(unsafeAdd(structAddr, field.Offset))
		var values = make([]*bigquery.QueryParameterValue, len(v))
		for i := range v {
			values[i] = &bigquery.QueryParameterValue{Value: strconv.FormatBool(v[i])}
		}
		return NewSliceQueryParameter(field.Name, values, paramTypeBool)
	}

	values[sliceIndexBegin+reflect.Int] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*[]int)(unsafeAdd(structAddr, field.Offset))
		var values = make([]*bigquery.QueryParameterValue, len(v))
		for i := range v {
			values[i] = &bigquery.QueryParameterValue{Value: strconv.Itoa(v[i])}
		}
		return NewSliceQueryParameter(field.Name, values, paramTypeInt)
	}

	values[sliceIndexBegin+reflect.Int8] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*[]int8)(unsafeAdd(structAddr, field.Offset))
		var values = make([]*bigquery.QueryParameterValue, len(v))
		for i := range v {
			values[i] = &bigquery.QueryParameterValue{Value: strconv.Itoa(int(v[i]))}
		}
		return NewSliceQueryParameter(field.Name, values, paramTypeInt)
	}

	values[sliceIndexBegin+reflect.Int16] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*[]int16)(unsafeAdd(structAddr, field.Offset))
		var values = make([]*bigquery.QueryParameterValue, len(v))
		for i := range v {
			values[i] = &bigquery.QueryParameterValue{Value: strconv.Itoa(int(v[i]))}
		}
		return NewSliceQueryParameter(field.Name, values, paramTypeInt)
	}

	values[sliceIndexBegin+reflect.Int32] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*[]int32)(unsafeAdd(structAddr, field.Offset))
		var values = make([]*bigquery.QueryParameterValue, len(v))
		for i := range v {
			values[i] = &bigquery.QueryParameterValue{Value: strconv.Itoa(int(v[i]))}
		}
		return NewSliceQueryParameter(field.Name, values, paramTypeInt)
	}

	values[sliceIndexBegin+reflect.Int64] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*[]int64)(unsafeAdd(structAddr, field.Offset))
		var values = make([]*bigquery.QueryParameterValue, len(v))
		for i := range v {
			values[i] = &bigquery.QueryParameterValue{Value: strconv.Itoa(int(v[i]))}
		}
		return NewSliceQueryParameter(field.Name, values, paramTypeInt)
	}

	values[sliceIndexBegin+reflect.Uint] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*[]uint)(unsafeAdd(structAddr, field.Offset))
		var values = make([]*bigquery.QueryParameterValue, len(v))
		for i := range v {
			values[i] = &bigquery.QueryParameterValue{Value: strconv.Itoa(int(v[i]))}
		}
		return NewSliceQueryParameter(field.Name, values, paramTypeInt)
	}

	values[sliceIndexBegin+reflect.Uint8] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*[]uint8)(unsafeAdd(structAddr, field.Offset))
		return NewBytesQueryParameter(field.Name, v)
	}

	values[sliceIndexBegin+reflect.Uint16] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*[]uint16)(unsafeAdd(structAddr, field.Offset))
		var values = make([]*bigquery.QueryParameterValue, len(v))
		for i := range v {
			values[i] = &bigquery.QueryParameterValue{Value: strconv.Itoa(int(v[i]))}
		}
		return NewSliceQueryParameter(field.Name, values, paramTypeInt)
	}

	values[sliceIndexBegin+reflect.Uint32] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*[]uint32)(unsafeAdd(structAddr, field.Offset))
		var values = make([]*bigquery.QueryParameterValue, len(v))
		for i := range v {
			values[i] = &bigquery.QueryParameterValue{Value: strconv.Itoa(int(v[i]))}
		}
		return NewSliceQueryParameter(field.Name, values, paramTypeInt)
	}

	values[sliceIndexBegin+reflect.Uint64] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*[]uint64)(unsafeAdd(structAddr, field.Offset))
		var values = make([]*bigquery.QueryParameterValue, len(v))
		for i := range v {
			values[i] = &bigquery.QueryParameterValue{Value: strconv.Itoa(int(v[i]))}
		}
		return NewSliceQueryParameter(field.Name, values, paramTypeInt)
	}

	values[sliceIndexBegin+reflect.Float32] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*[]float32)(unsafeAdd(structAddr, field.Offset))
		var values = make([]*bigquery.QueryParameterValue, len(v))
		for i := range v {
			values[i] = &bigquery.QueryParameterValue{Value: strconv.FormatFloat(float64(v[i]), 'f', -1, 64)}
		}
		return NewSliceQueryParameter(field.Name, values, paramTypeFloat64)
	}

	values[sliceIndexBegin+reflect.Float64] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*[]float64)(unsafeAdd(structAddr, field.Offset))
		var values = make([]*bigquery.QueryParameterValue, len(v))
		for i := range v {
			values[i] = &bigquery.QueryParameterValue{Value: strconv.FormatFloat(v[i], 'f', -1, 64)}
		}
		return NewSliceQueryParameter(field.Name, values, paramTypeFloat64)
	}

	values[sliceIndexBegin+reflect.String] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := *(*[]string)(unsafeAdd(structAddr, field.Offset))
		var values = make([]*bigquery.QueryParameterValue, len(v))
		for i := range v {
			values[i] = &bigquery.QueryParameterValue{Value: v[i]}
		}
		return NewSliceQueryParameter(field.Name, values, paramTypeString)
	}

	values[ptrIndexBegin+reflect.Bool] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := (*bool)(unsafeAdd(structAddr, field.Offset))
		return NewBoolPtrQueryParameter(field.Name, v)
	}

	values[ptrIndexBegin+reflect.Int] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := (*int)(unsafeAdd(structAddr, field.Offset))
		return NewIntPtrQueryParameter(field.Name, v)
	}

	values[ptrIndexBegin+reflect.Int8] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := (*int8)(unsafeAdd(structAddr, field.Offset))
		var i *int
		if v != nil {
			i = intPtr(int(*v))
		}
		return NewIntPtrQueryParameter(field.Name, i)
	}

	values[ptrIndexBegin+reflect.Int16] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := (*int16)(unsafeAdd(structAddr, field.Offset))
		var i *int
		if v != nil {
			i = intPtr(int(*v))
		}
		return NewIntPtrQueryParameter(field.Name, i)
	}

	values[ptrIndexBegin+reflect.Int32] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := (*int32)(unsafeAdd(structAddr, field.Offset))
		var i *int
		if v != nil {
			i = intPtr(int(*v))
		}
		return NewIntPtrQueryParameter(field.Name, i)
	}

	values[ptrIndexBegin+reflect.Int64] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := (*int64)(unsafeAdd(structAddr, field.Offset))
		var i *int
		if v != nil {
			i = intPtr(int(*v))
		}
		return NewIntPtrQueryParameter(field.Name, i)
	}

	values[ptrIndexBegin+reflect.Uint] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := (*uint)(unsafeAdd(structAddr, field.Offset))
		var i *int
		if v != nil {
			i = intPtr(int(*v))
		}
		return NewIntPtrQueryParameter(field.Name, i)
	}

	values[ptrIndexBegin+reflect.Uint8] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := (*uint8)(unsafeAdd(structAddr, field.Offset))
		var i *int
		if v != nil {
			i = intPtr(int(*v))
		}
		return NewIntPtrQueryParameter(field.Name, i)
	}

	values[ptrIndexBegin+reflect.Uint16] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := (*uint16)(unsafeAdd(structAddr, field.Offset))
		var i *int
		if v != nil {
			i = intPtr(int(*v))
		}
		return NewIntPtrQueryParameter(field.Name, i)
	}
	values[ptrIndexBegin+reflect.Uint32] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := (*uint32)(unsafeAdd(structAddr, field.Offset))
		var i *int
		if v != nil {
			i = intPtr(int(*v))
		}
		return NewIntPtrQueryParameter(field.Name, i)
	}

	values[ptrIndexBegin+reflect.Uint64] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := (*uint64)(unsafeAdd(structAddr, field.Offset))
		var i *int
		if v != nil {
			i = intPtr(int(*v))
		}
		return NewIntPtrQueryParameter(field.Name, i)
	}

	values[ptrIndexBegin+reflect.Float32] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := (*float32)(unsafeAdd(structAddr, field.Offset))
		var f64 *float64
		if v != nil {
			f := float64(*v)
			f64 = &f
		}
		return NewFloatPtrQueryParameter(field.Name, f64)
	}

	values[ptrIndexBegin+reflect.Float64] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := (*float64)(unsafeAdd(structAddr, field.Offset))
		return NewFloatPtrQueryParameter(field.Name, v)
	}

	values[ptrIndexBegin+reflect.String] = func(field reflect.StructField, structAddr unsafe.Pointer) (*bigquery.QueryParameter, error) {
		v := (*string)(unsafeAdd(structAddr, field.Offset))
		return NewStringPtrQueryParameter(field.Name, v)
	}

}

func valueKey(aType reflect.Type) reflect.Kind {
	key := aType.Kind()
	if key == reflect.Ptr {
		return aType.Elem().Kind() + ptrIndexBegin
	}
	if key == reflect.Slice {
		return aType.Elem().Kind() + sliceIndexBegin
	}
	return key
}

// NewBoolQueryParameter returns a bool query parameter
func NewBoolQueryParameter(name string, v bool) (*bigquery.QueryParameter, error) {
	return &bigquery.QueryParameter{
		Name:          name,
		ParameterType: paramTypeBool,
		ParameterValue: &bigquery.QueryParameterValue{
			Value: strconv.FormatBool(v),
		},
	}, nil
}

// NewIntQueryParameter returns an int query parameter
func NewIntQueryParameter(name string, v int) (*bigquery.QueryParameter, error) {
	return &bigquery.QueryParameter{
		Name:          name,
		ParameterType: paramTypeInt,
		ParameterValue: &bigquery.QueryParameterValue{
			Value: strconv.Itoa(v),
		},
	}, nil
}

// NewFloatQueryParameter returns a float query parameter
func NewFloatQueryParameter(name string, v float64) (*bigquery.QueryParameter, error) {
	return &bigquery.QueryParameter{
		Name:          name,
		ParameterType: paramTypeFloat64,
		ParameterValue: &bigquery.QueryParameterValue{
			Value: strconv.FormatFloat(v, 'f', -1, 64),
		},
	}, nil
}

// NewStringQueryParameter returns a string query parameter
func NewStringQueryParameter(name string, v string) (*bigquery.QueryParameter, error) {
	return &bigquery.QueryParameter{
		Name:          name,
		ParameterType: paramTypeString,
		ParameterValue: &bigquery.QueryParameterValue{
			Value:           v,
			ForceSendFields: []string{"Value"},
		},
	}, nil
}

// NewTimeQueryParameter returns a time query parameter
func NewTimeQueryParameter(name string, t *time.Time) (*bigquery.QueryParameter, error) {
	result := &bigquery.QueryParameter{
		Name:           name,
		ParameterType:  paramTypeTimestamp,
		ParameterValue: &bigquery.QueryParameterValue{},
	}

	if t != nil {
		result.ParameterValue.Value = t.Format(time.RFC3339Nano)
	}

	return result, nil
}

// NewBigNumericQueryParameter returns a big numeric query parameter
func NewBigNumericQueryParameter(name string, t *big.Rat) (*bigquery.QueryParameter, error) {
	result := &bigquery.QueryParameter{
		Name:           name,
		ParameterType:  paramTypeBigNumeric,
		ParameterValue: &bigquery.QueryParameterValue{},
	}
	if t != nil {
		result.ParameterValue.Value = t.String()
	}
	return result, nil
}

// NewBoolPtrQueryParameter returns an bool query parameter
func NewBoolPtrQueryParameter(name string, v *bool) (*bigquery.QueryParameter, error) {
	value := ""
	if v != nil {
		value = strconv.FormatBool(*v)
	}
	return &bigquery.QueryParameter{
		Name:          name,
		ParameterType: paramTypeBool,
		ParameterValue: &bigquery.QueryParameterValue{
			Value: value,
		},
	}, nil
}

// NewIntPtrQueryParameter returns an int query parameter
func NewIntPtrQueryParameter(name string, v *int) (*bigquery.QueryParameter, error) {
	value := ""
	if v != nil {
		value = strconv.Itoa(*v)
	}
	return &bigquery.QueryParameter{
		Name:          name,
		ParameterType: paramTypeInt,
		ParameterValue: &bigquery.QueryParameterValue{
			Value: value,
		},
	}, nil
}

// NewFloatPtrQueryParameter returns a float query parameter
func NewFloatPtrQueryParameter(name string, v *float64) (*bigquery.QueryParameter, error) {
	value := ""
	if v != nil {
		value = strconv.FormatFloat(*v, 'f', -1, 64)
	}
	return &bigquery.QueryParameter{
		Name:          name,
		ParameterType: paramTypeFloat64,
		ParameterValue: &bigquery.QueryParameterValue{
			Value: value,
		},
	}, nil
}

// NewStringPtrQueryParameter returns a string query parameter
func NewStringPtrQueryParameter(name string, v *string) (*bigquery.QueryParameter, error) {
	value := ""
	if v != nil {
		value = *v
	}
	result := &bigquery.QueryParameter{
		Name:          name,
		ParameterType: paramTypeString,
		ParameterValue: &bigquery.QueryParameterValue{
			Value: value,
		},
	}
	if value == "" {
		result.ParameterValue.ForceSendFields = []string{"Value"}
	}
	return result, nil
}

// NewBytesQueryParameter  returns bytes query parameter
func NewBytesQueryParameter(name string, v []byte) (*bigquery.QueryParameter, error) {
	actual := base64.StdEncoding.EncodeToString(v)
	return &bigquery.QueryParameter{
		Name:          name,
		ParameterType: paramTypeBytes,
		ParameterValue: &bigquery.QueryParameterValue{
			Value: actual,
		},
	}, nil
}

// NewSliceQueryParameter returns slice query parameters
func NewSliceQueryParameter(name string, values []*bigquery.QueryParameterValue, paramType *bigquery.QueryParameterType) (*bigquery.QueryParameter, error) {
	return &bigquery.QueryParameter{
		Name: name,
		ParameterType: &bigquery.QueryParameterType{
			ArrayType: paramType,
			Type:      "ARRAY",
		},
		ParameterValue: &bigquery.QueryParameterValue{
			ArrayValues: values,
		},
	}, nil
}

func intPtr(i int) *int {
	return &i
}
