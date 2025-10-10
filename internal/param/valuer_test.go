package param

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestDriverValuer tests that types implementing driver.Valuer are properly handled
func TestDriverValuer(t *testing.T) {
	t.Run("sql.NullBool with valid value", func(t *testing.T) {
		nb := sql.NullBool{Bool: true, Valid: true}
		param := New("test", nb)
		result, err := param.QueryParameter()

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "test", result.Name)
		assert.Equal(t, "BOOL", result.ParameterType.Type)
		assert.Equal(t, "true", result.ParameterValue.Value)
	})

	t.Run("sql.NullBool with null value", func(t *testing.T) {
		nb := sql.NullBool{Bool: false, Valid: false}
		param := New("test", nb)
		result, err := param.QueryParameter()

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "test", result.Name)
		assert.Equal(t, "BOOL", result.ParameterType.Type)
		assert.Equal(t, "", result.ParameterValue.Value)
	})

	t.Run("sql.NullInt64 with valid value", func(t *testing.T) {
		ni := sql.NullInt64{Int64: 42, Valid: true}
		param := New("test", ni)
		result, err := param.QueryParameter()

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "test", result.Name)
		assert.Equal(t, "INT64", result.ParameterType.Type)
		assert.Equal(t, "42", result.ParameterValue.Value)
	})

	t.Run("sql.NullInt64 with null value", func(t *testing.T) {
		ni := sql.NullInt64{Int64: 0, Valid: false}
		param := New("test", ni)
		result, err := param.QueryParameter()

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "test", result.Name)
		assert.Equal(t, "INT64", result.ParameterType.Type)
		assert.Equal(t, "", result.ParameterValue.Value)
	})

	t.Run("sql.NullFloat64 with valid value", func(t *testing.T) {
		nf := sql.NullFloat64{Float64: 3.14, Valid: true}
		param := New("test", nf)
		result, err := param.QueryParameter()

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "test", result.Name)
		assert.Equal(t, "FLOAT64", result.ParameterType.Type)
		assert.Equal(t, "3.14", result.ParameterValue.Value)
	})

	t.Run("sql.NullFloat64 with null value", func(t *testing.T) {
		nf := sql.NullFloat64{Float64: 0, Valid: false}
		param := New("test", nf)
		result, err := param.QueryParameter()

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "test", result.Name)
		assert.Equal(t, "FLOAT64", result.ParameterType.Type)
		assert.Equal(t, "", result.ParameterValue.Value)
	})

	t.Run("sql.NullString with valid value", func(t *testing.T) {
		ns := sql.NullString{String: "hello", Valid: true}
		param := New("test", ns)
		result, err := param.QueryParameter()

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "test", result.Name)
		assert.Equal(t, "STRING", result.ParameterType.Type)
		assert.Equal(t, "hello", result.ParameterValue.Value)
	})

	t.Run("sql.NullString with null value", func(t *testing.T) {
		ns := sql.NullString{String: "", Valid: false}
		param := New("test", ns)
		result, err := param.QueryParameter()

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "test", result.Name)
		assert.Equal(t, "STRING", result.ParameterType.Type)
		assert.Equal(t, "", result.ParameterValue.Value)
	})
}
