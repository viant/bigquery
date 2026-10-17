package bigquery

import (
	"context"
	"net/url"
	"testing"
)

func TestEmulatorConnectionConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, endpoint, extra string
		ok                    bool
	}{
		{"local", "http://127.0.0.1:19050/bigquery/v2/", "", true},
		{"ipv6", "http://[::1]:19050/bigquery/v2/", "", true},
		{"missing", "", "", false},
		{"cloud", "https://bigquery.googleapis.com/", "", false},
		{"remote", "http://example.com/", "", false},
		{"credentials", "http://localhost:19050/", "&credURL=must-not-be-read.json", false},
		{"api-key", "http://localhost:19050/", "&apiKey=example", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := ParseDSN("bigquery://ar-e2e/studies?emulator=true&endpoint=" + url.QueryEscape(tc.endpoint) + tc.extra)
			if (err == nil) != tc.ok {
				t.Fatalf("accepted=%v error=%v", err == nil, err)
			}
			if !tc.ok {
				return
			}
			connector, err := NewConnector(cfg)
			if err != nil {
				t.Fatal(err)
			}
			conn, err := connector.Connect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			conn.Close()
		})
	}
	cfg, err := ParseDSN("bigquery://ar-e2e/studies?endpoint=http%3A%2F%2F127.0.0.1%3A19050%2F")
	if err != nil || cfg.Emulator {
		t.Fatal("endpoint must not implicitly disable authentication")
	}
}
