package param

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"testing"
)

type jsonValue struct{ raw string }

func (v jsonValue) Value() (driver.Value, error) { return v.raw, nil }
func (v jsonValue) MarshalJSON() ([]byte, error) { return []byte(v.raw), nil }

type textValue string

func (v textValue) Value() (driver.Value, error) { return string(v), nil }
func TestJSONAndValuerParameters(t *testing.T) {
	raw := json.RawMessage(`{"id":9007199254741993,"zero":0,"enabled":false}`)
	for _, v := range []any{raw, &raw, jsonValue{string(raw)}} {
		p, err := New("attributes", v).QueryParameter()
		require.NoError(t, err)
		require.Equal(t, "JSON", p.ParameterType.Type)
		require.Equal(t, string(raw), p.ParameterValue.Value)
	}
	for _, v := range []any{json.RawMessage(nil), (*json.RawMessage)(nil)} {
		p, err := New("a", v).QueryParameter()
		require.NoError(t, err)
		encoded, err := json.Marshal(p)
		require.NoError(t, err)
		require.JSONEq(t, `{"name":"a","parameterType":{"type":"JSON"},"parameterValue":{}}`, string(encoded))
	}
	p, err := New("a", json.RawMessage("null")).QueryParameter()
	require.NoError(t, err)
	require.Equal(t, "null", p.ParameterValue.Value)
	_, err = New("a", json.RawMessage("{")).QueryParameter()
	require.Error(t, err)
	p, err = New("a", textValue("plain")).QueryParameter()
	require.NoError(t, err)
	require.Equal(t, "STRING", p.ParameterType.Type)
}

func TestValuerNilAndScalar(t *testing.T) {
	_, err := New("p", (*sql.NullString)(nil)).QueryParameter()
	require.Error(t, err)
	p, err := New("p", sql.NullString{String: "x", Valid: true}).QueryParameter()
	require.NoError(t, err)
	require.Equal(t, "x", p.ParameterValue.Value)
}
