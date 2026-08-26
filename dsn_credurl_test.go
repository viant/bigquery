package bigquery

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func TestMain(m *testing.M) {
	_, filename, _, ok := runtime.Caller(0)
	if ok {
		fakeOP := filepath.Join(filepath.Dir(filename), "testdata", "fake-op.sh")
		_ = os.Setenv("OP_CLI", fakeOP)
	}
	os.Exit(m.Run())
}

func TestParseDSN_credURLOp(t *testing.T) {
	opRef := "op://Private/viant-e2e.json/notesPlain"
	dsn := "bigquery://viant-e2e/mdp?credURL=" + url.QueryEscape(opRef)

	cfg, err := ParseDSN(dsn)
	require.NoError(t, err)
	require.True(t, cfg.hasCred())
	require.NotEmpty(t, cfg.CredentialJSON)

	var payload map[string]interface{}
	require.NoError(t, json.Unmarshal(cfg.CredentialJSON, &payload))
	assert.Equal(t, "service_account", payload["type"])
	assert.Equal(t, "test", payload["project_id"])

	opts := cfg.options()
	assert.True(t, isAuth(opts), "credURL-loaded JSON should produce explicit client credentials")
}

func TestIsAuth_explicitCredentials(t *testing.T) {
	cfg := &Config{CredentialJSON: []byte(`{"type":"service_account"}`)}
	assert.True(t, isAuth(cfg.options()))
}

func TestIsAuth_noExplicitCredentials(t *testing.T) {
	assert.False(t, isAuth(nil))
	assert.False(t, isAuth([]option.ClientOption{}))
}
