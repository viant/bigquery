package bigquery

import (
	"encoding/base64"
	"fmt"
	"google.golang.org/api/option"
	"net/url"
	"strings"
)

const (
	bigqueryScheme  = "bigquery"
	credentialsFile = "credentialsFile"
	credentialJSON  = "credentialJSON"
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
	CredentialJSON  string
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
	if c.CredentialJSON != "" {
		JSON := []byte(c.CredentialJSON)
		if raw, err := base64.RawURLEncoding.DecodeString(c.CredentialJSON); err == nil {
			JSON = raw
		}
		result = append(result, option.WithCredentialsJSON(JSON))
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
		if _, ok := cfg.Values[credentialsFile]; ok {
			cfg.CredentialsFile = cfg.Values.Get(credentialsFile)
		}
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
		if _, ok := cfg.Values[credentialJSON]; ok {
			cfg.CredentialJSON = cfg.Values.Get(credentialJSON)
		}
		if _, ok := cfg.Values[quotaProject]; ok {
			cfg.QuotaProject = cfg.Values.Get(quotaProject)
		}
		if _, ok := cfg.Values[scopes]; ok {
			cfg.Scopes = cfg.Values[scopes]
		}
	}
	if cfg.App == "" {
		cfg.App = defaultApp
	}
	if cfg.Location == "" {
		cfg.Location = "us"
	}
	return cfg, nil
}
