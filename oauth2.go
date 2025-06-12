package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	_ "github.com/viant/afs/mem"
	"github.com/viant/scy/auth"
	"github.com/viant/scy/auth/authorizer"
	"github.com/viant/scy/auth/flow"
	"github.com/viant/scy/auth/gcp/client"
	_ "github.com/viant/scy/kms/blowfish"

	"github.com/viant/afs/url"
	"github.com/viant/scy"
	"github.com/viant/scy/cred"
	"golang.org/x/oauth2"
	"time"
)

var defaultScopes = []string{
	"https://www.googleapis.com/auth/bigquery",
	"https://www.googleapis.com/auth/userinfo.email",
	"https://www.googleapis.com/auth/cloud-platform",
}

// OAuth2Manager represents oauth2 manager
type OAuth2Manager struct {
	secrets             *scy.Service
	fs                  afs.Service
	authorizer          *authorizer.Service
	newAuthCodeEndpoint func() (flow.Endpoint, error)
	baseURL             string
}

func (o *OAuth2Manager) Token(ctx context.Context, config *oauth2.Config, scopes ...string) (*oauth2.Token, error) {
	if config == nil {
		config = client.NewGCloud()
	}
	if len(scopes) == 0 {
		scopes = defaultScopes
	}
	anAuthorizer := authorizer.New()
	return anAuthorizer.Authorize(ctx, &authorizer.Command{
		OAuthConfig: authorizer.OAuthConfig{Config: config},
		AuthFlow:    "Browser",
		Scopes:      scopes,
		UsePKCE:     false,
		NewEndpoint: o.newAuthCodeEndpoint,
	})

}

// TokenSource returns oauth2.TokenSource constructed with provided config and token.
// The returned source automatically refreshes the supplied token when it expires.
// An error is returned if either cfg or token is nil.
func (o *OAuth2Manager) TokenSource(ctx context.Context, cfg *oauth2.Config, token *auth.Token) (oauth2.TokenSource, error) {
	if cfg == nil {
		return nil, fmt.Errorf("oauth2 config was nil")
	}
	if token == nil {
		return nil, fmt.Errorf("oauth2 token was nil")
	}

	// Ensure token expiry is set; if not provide far future to avoid immediate refresh in tests
	if token.Expiry.IsZero() {
		token.Expiry = time.Now().Add(time.Hour)
	}

	return cfg.TokenSource(ctx, &token.Token), nil
}

type OAuth2Option func(*OAuth2Manager)

func (o *OAuth2Manager) ConfigFromURL(ctx context.Context, URL string) (*oauth2.Config, error) {
	cfg := &cred.Oauth2Config{}
	resource := scy.NewResource(cfg, URL, "blowfish://default")
	secret, err := o.secrets.Load(ctx, resource)
	if err != nil {
		return nil, err
	}
	cfg, ok := secret.Target.(*cred.Oauth2Config)
	if !ok {
		return nil, fmt.Errorf("failed to load secret: %v", resource)
	}
	return &cfg.Config, nil
}

func (o *OAuth2Manager) TokenFromURL(ctx context.Context, URL string) (*auth.Token, error) {
	data, err := o.fs.DownloadWithURL(ctx, URL, file.DefaultFileOsMode)
	if err != nil {
		return nil, err
	}
	token := &auth.Token{}
	err = json.Unmarshal(data, &token)
	if err != nil {
		return nil, err
	}
	return token, nil
}

func (o *OAuth2Manager) WithConfigURL(ctx context.Context, config *oauth2.Config) (string, error) {
	URL := url.Join(o.baseURL, uuid.New().String())
	cfg := &cred.Oauth2Config{Config: *config}
	resource := scy.NewResource(cfg, URL, "blowfish://default")
	secret := scy.NewSecret(cfg, resource)
	if err := o.secrets.Store(ctx, secret); err != nil {
		return "", err
	}
	return URL, nil
}

func (o *OAuth2Manager) WithTokenURL(ctx context.Context, token *oauth2.Token) (string, error) {
	URL := url.Join(o.baseURL, uuid.New().String())
	data, err := json.Marshal(token)
	if err != nil {
		return URL, err
	}
	err = o.fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data))
	return URL, err
}

func WithNewAuthCodeEndpoint(f func() (flow.Endpoint, error)) OAuth2Option {
	return func(o *OAuth2Manager) {
		o.newAuthCodeEndpoint = f
	}
}

func NewOAuth2Manager(opts ...OAuth2Option) *OAuth2Manager {
	o := &OAuth2Manager{
		secrets:    scy.New(),
		fs:         afs.New(),
		authorizer: authorizer.New(),
		baseURL:    "mem://localhost/bigquery/",
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}
