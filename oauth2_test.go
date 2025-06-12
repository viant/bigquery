package bigquery

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/scy/auth"
	"golang.org/x/oauth2"
	"testing"
	"time"
)

func TestOauth2_TokenSource(t *testing.T) {
	type testCase struct {
		name      string
		cfg       *oauth2.Config
		token     *auth.Token
		expectErr bool
	}

	validCfg := &oauth2.Config{
		ClientID:     "clientID",
		ClientSecret: "clientSecret",
		Endpoint: oauth2.Endpoint{
			AuthURL:  "https://example.com/auth",
			TokenURL: "https://example.com/token",
		},
		RedirectURL: "https://example.com/redirect",
		Scopes:      []string{"scope1"},
	}

	validToken := &auth.Token{
		Token: oauth2.Token{
			AccessToken: "abc123",
			TokenType:   "Bearer",
			Expiry:      time.Now().Add(time.Hour),
		},
	}

	cases := []testCase{
		{
			name:  "valid token source",
			cfg:   validCfg,
			token: validToken,
		},
		{
			name:      "missing config",
			cfg:       nil,
			token:     validToken,
			expectErr: true,
		},
		{
			name:      "missing token",
			cfg:       validCfg,
			token:     nil,
			expectErr: true,
		},
	}

	ctx := context.Background()
	helper := NewOAuth2Manager()

	for _, tc := range cases {
		ts, err := helper.TokenSource(ctx, tc.cfg, tc.token)
		if tc.expectErr {
			assert.NotNil(t, err, tc.name)
			continue
		}
		assert.NoError(t, err, tc.name)
		assert.NotNil(t, ts, tc.name)

		gotToken, err := ts.Token()
		assert.NoError(t, err, tc.name)
		assert.EqualValues(t, tc.token.AccessToken, gotToken.AccessToken, tc.name)
	}
}
