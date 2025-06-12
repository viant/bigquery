# BigQuery SQL Driver

[![BigQuery database/sql driver in Go.](https://goreportcard.com/badge/github.com/viant/bigquery)](https://goreportcard.com/report/github.com/viant/bigquery)
[![GoDoc](https://godoc.org/github.com/viant/bigquery?status.svg)](https://godoc.org/github.com/viant/bigquery)

This library is compatible with Go 1.17+

Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [DSN](#dsn-data-source-name)
- [Usage](#usage)
- [Benchmark](#benchmark)
- [License](#license)
- [Credits and Acknowledgements](#credits-and-acknowledgements)
- [Custom OAuth 2.0 authentication](#custom-oauth-20-authentication)

This library provides fast implementation of the BigQuery Client as a database/sql drvier.

#### DSN Data Source Name

The BigQuery driver accepts the following DSN

* 'bigquery://projectID/[location/]datasetID?queryString'

  Where queryString can optionally configure the following option:
    - credURL: (url encoded) local location or URL supported by  [Scy](https://github.com/viant/scy)
    - credKey: optional (url encoded) [Scy](https://github.com/viant/scy) secret manager key or key location
    - credID: [Scy](https://github.com/viant/scy) resource secret ID
    - credJSON: rawURL base64 encoded cred JSON (not recommended)
    - endpoint
    - userAgent
    - apiKey
    - quotaProject
    - scopes

Since this library uses [Google Cloud API](google.golang.org/api/bigquery/v2)
you can pass your credentials via GOOGLE_APPLICATION_CREDENTIALS environment variable.

## Usage:

```go
package main

import (
	"database/sql"
	"fmt"
	"context"
	"log"
	_ "github.com/viant/bigquery"
)

type Participant struct {
	Name   string
	Splits []float64
}

func main() {

	db, err := sql.Open("bigquery", "bigquery://myProjectID/mydatasetID")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	SQL := `WITH races AS (
		  SELECT "800M" AS race,
		    [STRUCT("Ben" as name, [23.4, 26.3] as splits), 
		 	 STRUCT("Frank" as name, [23.4, 26.3] as splits)
			]
		       AS participants)
		SELECT
		  race,
		  participant
		FROM races r
		CROSS JOIN UNNEST(r.participants) as participant`

	rows, err := db.QueryContext(context.Background(), SQL)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
		var race string
		var participant Participant
		err = rows.Scan(&race, &participant)
		fmt.Printf("fetched: %v %+v\n", race, participant)
		if err != nil {
			log.Fatal(err)
		}
	}
}
```

## Data Ingestion (Load/Stream)

This driver implements LOAD/STREAM operation with the following SQL:

### Loading data

To load data register a reader for supported source format, followed by LOAD SQL.

```sql
LOAD 'Reader:<SOURCE_FORMAT>:<READER_ID>' DATA INTO TABLE myproject.mydataset.mytable
```

The following snippet register READER_ID

```go
 err := reader.Register(readerID, myReader)
```

The following code loads CSV data

```go
package mypkg

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/bigquery/reader"
	"log"
	"strings"
)

func ExampleOfCSVLoad() {
	projectID := "my-gcp-project"
	db, err := sql.Open("bigquery", fmt.Sprintf("bigquery://%v/test", projectID))
	if err != nil {
		log.Fatal(err)
	}
	readerID := "123456"
	csvReader := strings.NewReader("ID,Name,Desc\n1,Name 1,Test\n2,Name 2,Test 2")
	err = reader.Register(readerID, csvReader)
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Unregister(readerID)
	SQL := fmt.Sprintf(`LOAD 'Reader:csv:%v' DATA INTO TABLE mytable`, readerID)
	result, err := db.ExecContext(context.TODO(), SQL)
	if err != nil {
		log.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	fmt.Printf("loaded: %v rows", affected)
}
```

## Custom OAuth 2.0 authentication

Sometimes you already have a **Google** OAuth 2.0 client (client-id / secret) and an access or refresh token that
you would like the driver to use.  
`OAuth2Manager` can serialise such an `oauth2.Config` together with the corresponding `oauth2.Token` and make them
addressable via URLs that you embed in the DSN.

### Step-by-step example

```go
package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    "log"
    "net/url"
    "time"

    _ "github.com/viant/bigquery"
    "github.com/viant/bigquery"

    "golang.org/x/oauth2"
)

func main() {
    projectID := "my-project"
    datasetID := "my_dataset"

    // 1. Build the OAuth2 client configuration. In real life you would usually
    //    load this from a secret manager or configuration file.
    // IMPORTANT: the endpoints **must** be Google's authorization server
    // endpoints; BigQuery only accepts Google-issued access tokens.
    oauthCfg := &oauth2.Config{
        ClientID:     "<CLIENT_ID>",
        ClientSecret: "<CLIENT_SECRET>",
        Endpoint: oauth2.Endpoint{
            AuthURL:  "https://accounts.google.com/o/oauth2/v2/auth",
            TokenURL: "https://oauth2.googleapis.com/token",
        },
        RedirectURL: "http://localhost/oauth2callback",
        Scopes:      []string{"https://www.googleapis.com/auth/bigquery"},
    }

    // 2. Provide a *valid* access or refresh token obtained beforehand.
    //    The token expiry _must_ be set so that the helper can decide when to
    //    refresh it.
    oauthToken := &oauth2.Token{
        AccessToken:  "<ACCESS_TOKEN>",
        RefreshToken: "<REFRESH_TOKEN>", // optional – only when you want automatic refresh
        TokenType:    "Bearer",
        Expiry:       time.Now().Add(time.Hour),
    }

    // 3. Serialise both config and token to in-memory URLs that can be used in
    //    the DSN. You may also store them in any other fs supported by Afero
    //    (e.g. Google Cloud Storage, local disk, S3, …).
    helper := bigquery.NewOAuth2Manager()
    ctx := context.Background()

    cfgURL, err := helper.WithConfigURL(ctx, oauthCfg)
    if err != nil {
        log.Fatalf("failed to persist oauth2 config: %v", err)
    }

    tokenURL, err := helper.WithTokenURL(ctx, oauthToken)
    if err != nil {
        log.Fatalf("failed to persist oauth2 token: %v", err)
    }

    // 4. Build the DSN using the two generated URLs. Be sure to URL-escape them
    //    because the DSN itself is an URL as well.
    dsn := fmt.Sprintf(
        "bigquery://%s/%s?oauth2ClientURL=%s&oauth2TokenURL=%s",
        projectID,
        datasetID,
        url.QueryEscape(cfgURL),
        url.QueryEscape(tokenURL),
    )

    db, err := sql.Open("bigquery", dsn)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Now use the db as usual – the driver will pick the token source built
    // from your custom OAuth2 config and token and will automatically refresh
    // the access token when it expires.
    var version string
    if err := db.QueryRowContext(ctx, "SELECT version()" ).Scan(&version); err != nil {
        log.Fatal(err)
    }
    fmt.Printf("connected, BigQuery engine version: %s\n", version)

    // For demonstration purposes print the DSN with the embedded URLs so that
    // you can see what happens under the hood.
    if raw, _ := json.MarshalIndent(struct {
        DSN       string `json:"dsn"`
        ConfigURL string `json:"oauth2ClientURL"`
        TokenURL  string `json:"oauth2TokenURL"`
    }{dsn, cfgURL, tokenURL}, "", "  "); raw != nil {
        fmt.Printf("%s\n", raw)
    }
}
```

**Important notes**

1. `OAuth2Manager` uses an in-memory ( `mem://` ) file-system backend by default.  
   The URLs produced in the example therefore live only for the lifetime of the
   current process.  If you want to persist them, provide your own `afs.Service`
   or store the JSON blobs yourself and pass their URLs in the DSN – the driver
   only requires that the URLs are reachable, not that they were created by
   `OAuth2Manager`.
2. The provided token **must** have the `Expiry` field set.  When the expiry is
   reached the token will be refreshed automatically using the refresh token
   and the OAuth2 endpoint supplied in the config.
3. The `oauth2.Config.Endpoint` **must** point to Google's authorization server
   (`https://accounts.google.com/o/oauth2/v2/auth` and
   `https://oauth2.googleapis.com/token`).  BigQuery accepts only Google-issued
   access tokens.


### Loading application data

The following code loads CSV data

```go
package mypkg

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/bigquery/reader"
	"github.com/viant/sqlx/io/load/reader/parquet"
	"github.com/google/uuid"
	"log"
)

type Record struct {
	ID     int     `parquet:"id,plain,optional"`
	Name   string  `parquet:"name,plain,optional"`
	Active bool    `parquet:"active,plain,optional"`
	Amount float64 `parquet:"amount,plain,optional"`
}

func ExampleOfAppDataLoad() {
	projectID := "my-gcp-project"
	db, err := sql.Open("bigquery", fmt.Sprintf("bigquery://%v/test", projectID))
	if err != nil {
		log.Fatal(err)
	}
	var records = []*Record{
		{
			ID:     1,
			Name:   "record 1",
			Amount: 12.2,
		},
		{
			ID:     2,
			Name:   "record 2",
			Amount: 12.3,
		},
	}
	readerID := uuid.New().String()
    parquetReader, err := parquet.NewReader(records)
    if err != nil {
        log.Fatal(err)
    }
	err = reader.Register(readerID, parquetReader)
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Unregister(readerID)
	SQL := fmt.Sprintf(`LOAD 'Reader:parquet:%v' DATA INTO TABLE mytable`, readerID)
	result, err := db.ExecContext(context.TODO(), SQL)
	if err != nil {
		log.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	fmt.Printf("loaded: %v rows", affected)
}
```



### Load option control

To customize Load you can inline
[JobConfigurationLoad](https://github.com/googleapis/google-api-go-client/blob/main/bigquery/v2/bigquery-gen.go#L4297)
you can
as JSON as a hint
``` LOAD 'Reader:<SOURCE_FORMAT>:<READER_ID>' /*+ <HINT> +*/ DATA INTO TABLE mytable```
for example:

```sql
LOAD 'Reader:CSV:201F973D-9BAB-4E0A-880F-7830B876F210' /*+ {
    "AllowJaggedRows": true,
    "AllowQuotedNewlines":true
  } +*/  DATA INTO TABLE mytable
```


## Benchmark

Benchmark runs 3 times the following queries:

- primitive types:

```sql 
   SELECT state,gender,year,name,number 
   FROM `bigquery-public-data.usa_names.usa_1910_2013` LIMIT 10000
```

- complex type (repeated string/repeated record)

```sql 
   SELECT  t.publication_number, t.inventor, t.assignee, t.description_localized 
   FROM `patents-public-data.patents.publications_202105` t ORDER BY 1 LIMIT 1000
```

In both case database/sql driver is faster and allocate way less memory
than [GCP BigQuery API Client](https://cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-go)

```go
goos: darwin
goarch: amd64
pkg: github.com/viant/bigquery/bench
cpu: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz
Benchmark_Primitive_GCPClient
database/gcp: primitive types 3.918388531s
Benchmark_Primitive_GCPClient-16               1    3918442974 ns/op    42145144 B/op      830647 allocs/op
Benchmark_Primitive_SQLDriver
database/sql: primitive types 2.880091637s
Benchmark_Primitive_SQLDriver-16               1    2880149491 ns/op    22301848 B/op      334547 allocs/op
Benchmark_Complex_GCPClient
database/gcp: structured types 3.303497894s
Benchmark_Complex_GCPClient-16               1    3303548761 ns/op    11551216 B/op      154660 allocs/op
Benchmark_Complex_SQLDriver
database/sql: structured types 2.690012577s
Benchmark_Complex_SQLDriver-16               1    2690056675 ns/op     6643176 B/op       71562 allocs/op
PASS
```

## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.

## Credits and Acknowledgements

**Library Author:** Adrian Witas
