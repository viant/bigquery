package bigquery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
)

func TestIsAuth_withCredentialsJSON(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent/viant-bigquery-isauth-test.json")

	valid := []byte(`{"type":"service_account","project_id":"test"}`)
	assert.True(t, isAuth([]option.ClientOption{option.WithCredentialsJSON(valid)}))
}

func TestIsAuth_emptyOptions(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent/viant-bigquery-isauth-test.json")

	assert.False(t, isAuth(nil))
	assert.False(t, isAuth([]option.ClientOption{}))
}
