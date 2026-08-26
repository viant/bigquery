//go:build integration

package bigquery

import (
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIntegrationParseDSN_credURLOp loads a real 1Password secret through ParseDSN.
//
// Run manually (not from CI or Cursor):
//
//	op signin
//	export OP_INTEGRATION_REF='op://Private/viant-e2e.json/notesPlain'
//	go test . -tags=integration -run TestIntegrationParseDSN_credURLOp -count=1 -v
func TestIntegrationParseDSN_credURLOp(t *testing.T) {
	ref := os.Getenv("OP_INTEGRATION_REF")
	if ref == "" {
		t.Skip("set OP_INTEGRATION_REF to an op:// secret reference")
	}

	dsn := "bigquery://viant-e2e/mdp?credURL=" + url.QueryEscape(ref)
	cfg, err := ParseDSN(dsn)
	require.NoError(t, err)
	require.True(t, cfg.hasCred())
	require.NotEmpty(t, cfg.CredentialJSON)
	require.True(t, isAuth(cfg.options()))
	t.Logf("read %d bytes via credURL", len(cfg.CredentialJSON))
}
