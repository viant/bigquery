package bigquery

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/viant/scy"
	"google.golang.org/api/option"
	"net/url"
	"strings"
)

const (
	bigqueryScheme  = "bigquery"
	credentialsJSON = "credJSON"
	credentialsURL  = "credURL"
	credentialsKey  = "credKey"
	credID          = "credID"
	endpoint        = "endpoint"
	userAgent       = "ua"
	apiKey          = "apiKey"
	quotaProject    = "quotaProject"
	scopes          = "scopes"
	app             = "app"
	defaultApp      = "go-sql-bq"
)

// Config is a configuration parsed from a DSN string.
// If a new Config is created instead of being parsed from a DSN string,
// the NewConfig function should be used, which sets default values.
type Config struct {
	CredentialsFile string // Username
	Endpoint        string
	APIKey          string
	CredentialJSON  []byte
	CredentialsURL  string
	CredID          string //scy secret resource ID
	CredentialsKey  string
	UserAgent       string
	ProjectID       string // project ID
	DatasetID       string
	QuotaProject    string
	Scopes          []string
	Location        string
	App             string
	url.Values
}

func (c *Config) options() []option.ClientOption {
	var result = make([]option.ClientOption, 0)
	if c.CredentialsFile != "" {
		result = append(result, option.WithCredentialsFile(c.CredentialsFile))
	}
	if c.Endpoint != "" {
		result = append(result, option.WithEndpoint(c.Endpoint))
	}
	if c.UserAgent != "" {
		result = append(result, option.WithEndpoint(c.UserAgent))
	}
	if c.APIKey != "" {
		result = append(result, option.WithAPIKey(c.APIKey))
	}
	if c.QuotaProject != "" {
		result = append(result, option.WithQuotaProject(c.QuotaProject))
	}
	if len(c.CredentialJSON) > 0 {
		result = append(result, option.WithCredentialsJSON(c.CredentialJSON))
	}

	if len(c.Scopes) > 0 {
		result = append(result, option.WithScopes(c.Scopes...))
	}
	return result
}

// NewConfig creates a new Config and sets default values.
func NewConfig() *Config {
	return &Config{}
}

// ParseDSN parses the DSN string to a Config
func ParseDSN(dsn string) (*Config, error) {
	URL, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid dsn: %v", err)
	}
	if URL.Scheme != bigqueryScheme {
		return nil, fmt.Errorf("invalid dsn scheme, expected %v, but had: %v", bigqueryScheme, URL.Scheme)
	}

	path := strings.Trim(URL.Path, "/")
	location := ""
	if index := strings.Index(path, "/"); index != -1 {
		location = path[:index]
		path = path[index+1:]
	}
	cfg := &Config{
		ProjectID: URL.Host,
		DatasetID: path,
		Location:  location,
		Values:    URL.Query(),
	}
	if len(cfg.Values) > 0 {
		if _, ok := cfg.Values[endpoint]; ok {
			cfg.Endpoint = cfg.Values.Get(endpoint)
		}
		if _, ok := cfg.Values[userAgent]; ok {
			cfg.UserAgent = cfg.Values.Get(userAgent)
		}
		if _, ok := cfg.Values[apiKey]; ok {
			cfg.APIKey = cfg.Values.Get(apiKey)
		}
		if _, ok := cfg.Values[app]; ok {
			cfg.App = cfg.Values.Get(app)
		}
		if _, ok := cfg.Values[credID]; ok {
			cfg.CredID = cfg.Values.Get(credID)
		}
		if _, ok := cfg.Values[credentialsJSON]; ok {
			cfg.CredentialJSON = []byte(cfg.Values.Get(credentialsJSON))
		}
		if _, ok := cfg.Values[credentialsKey]; ok {
			cfg.CredentialsKey = cfg.Values.Get(credentialsKey)
		}
		if _, ok := cfg.Values[credentialsURL]; ok {
			cfg.CredentialsURL = cfg.Values.Get(credentialsURL)
		}
		if _, ok := cfg.Values[quotaProject]; ok {
			cfg.QuotaProject = cfg.Values.Get(quotaProject)
		}
		if _, ok := cfg.Values[scopes]; ok {
			cfg.Scopes = cfg.Values[scopes]
		}
	}

	if cfg.CredentialsKey != "" {
		if URL, err := base64.RawURLEncoding.DecodeString(cfg.CredentialsKey); err == nil {
			cfg.CredentialsKey = string(URL)
		}
	}

	if err = cfg.initialiseSecrets(); err != nil {
		return nil, err
	}

	if cfg.App == "" {
		cfg.App = defaultApp
	}
	if cfg.Location == "" {
		cfg.Location = "us"
	}
	return cfg, nil
}

func (c *Config) initialiseSecrets() error {
	if c.CredentialsURL != "" {
		if URL, err := base64.RawURLEncoding.DecodeString(c.CredentialsURL); err == nil {
			c.CredentialsURL = string(URL)
		}
	}
	if c.CredentialsKey != "" {
		if URL, err := base64.RawURLEncoding.DecodeString(c.CredentialsKey); err == nil {
			c.CredentialsKey = string(URL)
		}
	}
	if len(c.CredentialJSON) > 0 {
		if raw, err := base64.RawURLEncoding.DecodeString(string(c.CredentialJSON)); err == nil {
			c.CredentialJSON = raw
		}
	}
	if c.CredID != "" {
		resource := scy.Resources().Lookup(c.CredID)
		if resource == nil {
			return fmt.Errorf("failed to lookup secretID: %v", c.CredID)
		}
		secrets, err := c.loadSecret(resource)
		if err != nil {
			return err
		}
		c.CredentialJSON = []byte(secrets.String())
	}

	if c.CredentialsURL != "" {
		secrets, err := c.loadSecret(&scy.Resource{URL: c.CredentialsURL, Key: c.CredentialsKey})
		if err != nil {
			return err
		}
		c.CredentialJSON = []byte(secrets.String())
	}
	return nil
}

func (c *Config) loadSecret(resource *scy.Resource) (*scy.Secret, error) {
	secretsMgr := scy.New()
	secrets, err := secretsMgr.Load(context.Background(), resource)
	if err != nil {
		return nil, fmt.Errorf("failed to load secret from :%v, %w", resource.URL, err)
	}
	return secrets, nil
}
