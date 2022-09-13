package query

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/francoispqt/gojay"
	"github.com/viant/bigquery/internal"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"io"
	"net/http"
	"unsafe"
)

// ResultsCall represents query results call
type ResultsCall struct {
	session *internal.Session
	*nativeCall
}

// nativeCall represents original *bigquery.JobsGetQueryResultsCall (fields order and data type has to match)
type nativeCall struct {
	s            *bigquery.Service
	ProjectID    string
	JobID        string
	urlParamsX   URLParams
	ifNoneMatchX string
	ctxX         context.Context
	headerX      http.Header
}

// Context sets a context
func (c *ResultsCall) Context(ctx context.Context) {
	c.ctxX = ctx
}

// Do runs a call
func (c *ResultsCall) Do(opts ...googleapi.CallOption) (*Response, error) {

	SetOptions(c.urlParamsX, opts...)

	res, err := c.doRequest("json")
	if res.Body != nil {
		data, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}
		c.session.Data = data
		res.Body = io.NopCloser(bytes.NewReader(data))
	}

	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}

	ret := &Response{
		session: c.session,
		QueryResponse: bigquery.QueryResponse{
			ServerResponse: googleapi.ServerResponse{
				Header:         res.Header,
				HTTPStatusCode: res.StatusCode,
			},
		},
	}

	c.session.Rows = []internal.Region{}
	err = gojay.UnmarshalJSONObject(c.session.Data, ret)
	if err != nil {
		return nil, fmt.Errorf("failed to parseJSON: %w, %s", err, c.session.Data)
	}
	return ret, nil
}

func (c *nativeCall) httpClient() *http.Client {
	client := *(**http.Client)(unsafe.Pointer(c.s))
	return client
}

func (c *nativeCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	reqHeaders.Set("x-goog-api-client", "viant/bigquery")
	for k, v := range c.headerX {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", "GoLang")
	if c.ifNoneMatchX != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatchX)
	}
	var body io.Reader = nil
	c.urlParamsX.Set("alt", alt)
	c.urlParamsX.Set("prettyPrint", "false")
	urls := googleapi.ResolveRelative(c.s.BasePath, "projects/{projectId}/queries/{jobId}")
	urls += "?" + c.urlParamsX.Encode()
	req, err := http.NewRequest("GET", urls, body)
	if err != nil {
		return nil, err
	}
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"projectId": c.ProjectID,
		"jobId":     c.JobID,
	})
	return sendRequest(c.ctxX, c.httpClient(), req)
}

func sendRequest(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
	// Disallow Accept-Encoding because it interferes with the automatic gzip handling
	// done by the default http.Transport. See https://github.com/google/google-api-go-client/issues/219.
	if _, ok := req.Header["Accept-Encoding"]; ok {
		return nil, errors.New("google api: custom Accept-Encoding headers not allowed")
	}
	if ctx == nil {
		return client.Do(req)
	}
	// Send request.
	resp, err := send(ctx, client, req)
	return resp, err
}

func send(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req.WithContext(ctx))
	// If we got an error, and the context has been canceled,
	// the context's error is probably more useful.
	if err != nil {
		select {
		case <-ctx.Done():
			err = ctx.Err()
		default:
		}
	}
	return resp, err
}

// NewResultsCall creates a new query result call
func NewResultsCall(call *bigquery.JobsGetQueryResultsCall, session *internal.Session) *ResultsCall {
	res := (*nativeCall)(unsafe.Pointer(call))
	return &ResultsCall{nativeCall: res, session: session}
}
