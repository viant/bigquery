package bigquery

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Synthetic user credentials exercise client construction without contacting OAuth
// or BigQuery. Token exchange is intentionally not part of this offline test.
func TestApplicationDefaultUserCredentialsOptIn(t *testing.T) {
	file := filepath.Join(t.TempDir(), "user-adc.json")
	require.NoError(t, os.WriteFile(file, []byte(`{"type":"authorized_user","client_id":"example-client","client_secret":"example-secret","refresh_token":"example-refresh-token"}`), 0600))
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", file)
	t.Setenv("BIGQUERY_ALLOW_BROWSER_AUTH", "false")
	cfg, err := ParseDSN("bigquery://example-project/example_dataset")
	require.NoError(t, err)
	require.False(t, cfg.hasCred(), "ADC is selected by the explicit environment setting, not injected into the DSN")
	connection, err := (&connector{cfg: cfg}).Connect(context.Background())
	require.NoError(t, err)
	require.NotNil(t, connection)
	require.NoError(t, connection.Close())
}
