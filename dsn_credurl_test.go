package bigquery

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func TestParseDSN_credURLOp(t *testing.T) {
	opRef := "op://Private/e2e-account.json/notesPlain"
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
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	cfg := &Config{CredentialJSON: []byte(`{"type":"service_account"}`)}
	assert.True(t, isAuth(cfg.options()))
}

func TestIsAuth_applicationDefaultCredentials(t *testing.T) {
	credFile := filepath.Join(t.TempDir(), "adc.json")
	const saJSON = `{
		"type": "service_account",
		"project_id": "test",
		"private_key_id": "key-id",
		"private_key": "-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBALRiLoqFqmQvQ2s3WCAS23iR4fa9XgInC4NNRLTaPaJoV1XW5l0C\nAwEAAQJAZG7q2B9WQHq+8sL0Z3uGJm6n8f5WQHq+8sL0Z3uGJm6n8f5WQHq+8sL0Z\n3uGJm6n8f5WQHq+8sL0Z3uGJm6n8f5WQHq+8sL0Z3uGJm6n8f5WQHq+8sL0Z3uG\nJm6n8f5WQHq+8sL0Z3uGJm6n8f5Q==\n-----END RSA PRIVATE KEY-----\n",
		"client_email": "test@test.iam.gserviceaccount.com",
		"client_id": "123456789",
		"auth_uri": "https://accounts.google.com/o/oauth2/auth",
		"token_uri": "https://oauth2.googleapis.com/token",
		"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
		"client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test%40test.iam.gserviceaccount.com"
	}`
	require.NoError(t, os.WriteFile(credFile, []byte(saJSON), 0600))
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credFile)

	assert.True(t, isAuth(nil), "isAuth must detect GOOGLE_APPLICATION_CREDENTIALS when client options are empty")
	assert.True(t, isAuth([]option.ClientOption{}))
}

func TestIsAuth_noExplicitCredentials(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	if isAuth(nil) {
		t.Skip("application default credentials available without GOOGLE_APPLICATION_CREDENTIALS in this environment")
	}
	assert.False(t, isAuth(nil))
	assert.False(t, isAuth([]option.ClientOption{}))
}
