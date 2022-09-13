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

//SetOptions sets global client options
func SetOptions(opts ...option.ClientOption) {
	globalOptions = opts
}

//Connect connects to database
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	options := c.cfg.options()
	if len(c.options) > 0 {
		options = append(options, c.options...)
	} else if len(globalOptions) > 0 {
		options = append(options, globalOptions...)
	}
	isAuth := isAuth(options)

	if !c.cfg.hasCred() && !isAuth {
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
	credentials, _ := google.FindDefaultCredentials(context.Background())
	if credentials != nil {
		return true
	}
	if len(options) == 0 {
		return false
	}
	for _, opt := range options {
		if _, ok := opt.(oauth2.TokenSource); ok {
			return ok
		}
		if _, ok := opt.(oauth2.TokenSource); ok {
			return ok
		}
		optName := reflect.TypeOf(opt).String()
		if strings.Contains(optName, "HTTP") || strings.Contains(optName, "Creds") {
			return true
		}
	}
	return false
}

//Driver returns a driver
func (c *connector) Driver() driver.Driver {
	return &Driver{}
}
