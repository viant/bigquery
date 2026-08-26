package bigquery

import (
	"context"
	"database/sql/driver"
	"github.com/viant/scy/auth/gcp"
	"github.com/viant/scy/auth/gcp/client"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
	"reflect"
	"strings"
)

type connector struct {
	cfg     *Config
	options []option.ClientOption
}

var globalOptions []option.ClientOption

// SetOptions sets global client options
func SetOptions(opts ...option.ClientOption) {
	globalOptions = opts
}

// Connect connects to database
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	options := c.cfg.options()

	//If both OAuth2 token and config URLs are provided, build token source and use it.
	tokenSourceProvided := false
	if c.cfg.OAuth2ConfigURL != "" && c.cfg.OAuth2TokenURL != "" {
		helper := NewOAuth2Manager()
		oauthCfg, err := helper.ConfigFromURL(ctx, c.cfg.OAuth2ConfigURL)
		if err != nil {
			return nil, err
		}
		oauthToken, err := helper.TokenFromURL(ctx, c.cfg.OAuth2TokenURL)
		if err != nil {
			return nil, err
		}
		src, err := helper.TokenSource(ctx, oauthCfg, oauthToken)
		if err != nil {
			return nil, err
		}
		options = append(options, option.WithTokenSource(src))
		tokenSourceProvided = true
	}

	if len(c.options) > 0 {
		options = append(options, c.options...)
	} else if len(globalOptions) > 0 {
		options = append(options, globalOptions...)
	}

	isAuthOpt := isAuth(options)
	if tokenSourceProvided {
		isAuthOpt = true
	}

	if !c.cfg.hasCred() && !isAuthOpt {
		gcpService := gcp.New(client.NewGCloud())
		httpClient, err := gcpService.AuthClient(context.Background(), append(gcp.Scopes, "https://www.googleapis.com/auth/bigquery")...)
		if err == nil && httpClient != nil {
			options = append(options, option.WithHTTPClient(httpClient))
		}
	}

	service, err := bigquery.NewService(ctx, options...)
	if err != nil {
		return nil, err
	}
	return &connection{
		ctx:       ctx,
		service:   service,
		cfg:       c.cfg,
		projectID: c.cfg.ProjectID,
	}, nil
}

func isAuth(options []option.ClientOption) bool {
	for _, opt := range options {
		if _, ok := opt.(oauth2.TokenSource); ok {
			return true
		}
		optName := reflect.TypeOf(opt).String()
		if strings.Contains(optName, "Credentials") || strings.Contains(optName, "HTTP") {
			return true
		}
	}
	credentials, _ := google.FindDefaultCredentials(context.Background())
	return credentials != nil
}

// Driver returns a driver
func (c *connector) Driver() driver.Driver {
	return &Driver{}
}
