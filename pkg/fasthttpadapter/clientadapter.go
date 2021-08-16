package fasthttpadapter

import (
	"bytes"
	"context"
	"time"

	"github.com/lucasanjosmoraes/chicago-ports/pkg/http"
	"github.com/lucasanjosmoraes/chicago-toolkit/pkg/benchmark"
	"github.com/valyala/fasthttp"
)

// ClientConfig stores all client configuration variables.
type ClientConfig struct {
	UserAgent string
}

// ClientAdapter is a Client implementation based on fasthttp dependency.
type ClientAdapter struct {
	client               *fasthttp.Client
	BenckmarkIntegration benchmark.Integration
}

// NewClient finishes all needed initialization of the dependency.
func NewClient(c ClientConfig, b benchmark.Integration) http.Client {
	return &ClientAdapter{
		client: &fasthttp.Client{
			Name:                     c.UserAgent,
			NoDefaultUserAgentHeader: true,
			MaxIdleConnDuration:      30 * time.Second,
			MaxConnDuration:          30 * time.Second,
			ReadTimeout:              5 * time.Second,
			WriteTimeout:             10 * time.Second,
			MaxResponseBodySize:      1 << 20,
		},
		BenckmarkIntegration: b,
	}
}

// Delete executes a DELETE request calling the given URL, using the given headers.
func (a ClientAdapter) Delete(ctx context.Context, url string, headers []http.Header) (http.ClientResponse, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodDelete)
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Accept", "application/json")

	for _, header := range headers {
		req.Header.Set(header.Key, header.Value)
	}

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	defer a.BenckmarkIntegration.Took(benchmark.WithRequest(ctx, "Delete", url))

	err := fasthttp.Do(req, resp)
	if err != nil {
		return http.ClientResponse{}, err
	}

	body, err := getBodyEvenIfCompressed(resp)

	return http.ClientResponse{
		StatusCode: resp.StatusCode(),
		Body:       body,
	}, err
}

// Get executes a GET request calling the given URL, using the given headers.
func (a ClientAdapter) Get(ctx context.Context, url string, headers []http.Header) (http.ClientResponse, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodGet)
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Accept", "application/json")

	for _, header := range headers {
		req.Header.Set(header.Key, header.Value)
	}

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	defer a.BenckmarkIntegration.Took(benchmark.WithRequest(ctx, "Get", url))

	err := fasthttp.Do(req, resp)
	if err != nil {
		return http.ClientResponse{}, err
	}

	body, err := getBodyEvenIfCompressed(resp)

	return http.ClientResponse{
		StatusCode: resp.StatusCode(),
		Body:       body,
	}, err
}

// Patch executes a PATCH request calling the given URL, using the given headers and body.
func (a ClientAdapter) Patch(ctx context.Context, url string, body []byte, headers []http.Header) (http.ClientResponse, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodPatch)
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Accept", "application/json")
	req.Header.SetContentType("application/json")

	for _, header := range headers {
		req.Header.Set(header.Key, header.Value)
	}

	req.SetBody(body)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	defer a.BenckmarkIntegration.Took(benchmark.WithRequest(ctx, "Patch", url))

	err := fasthttp.Do(req, resp)
	if err != nil {
		return http.ClientResponse{}, err
	}

	respBody, err := getBodyEvenIfCompressed(resp)

	return http.ClientResponse{
		StatusCode: resp.StatusCode(),
		Body:       respBody,
	}, err
}

// Post executes a POST request calling the given URL, using the given headers and body.
func (a ClientAdapter) Post(ctx context.Context, url string, body []byte, headers []http.Header) (http.ClientResponse, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Accept", "application/json")
	req.Header.SetContentType("application/json")

	for _, header := range headers {
		req.Header.Set(header.Key, header.Value)
	}

	req.SetBody(body)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	defer a.BenckmarkIntegration.Took(benchmark.WithRequest(ctx, "Post", url))

	err := fasthttp.Do(req, resp)
	if err != nil {
		return http.ClientResponse{}, err
	}

	respBody, err := getBodyEvenIfCompressed(resp)

	return http.ClientResponse{
		StatusCode: resp.StatusCode(),
		Body:       respBody,
	}, err
}

// PostForm executes a POST request calling the given URL, using the given headers,
// formHeader (related to the form content-type) and body.
func (a ClientAdapter) PostForm(ctx context.Context, url string, body *bytes.Buffer, formHeader string, headers []http.Header) (http.ClientResponse, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.Set("Accept", "application/json")
	req.Header.SetContentType(formHeader)

	for _, header := range headers {
		req.Header.Set(header.Key, header.Value)
	}

	req.SetBody(body.Bytes())

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	defer a.BenckmarkIntegration.Took(benchmark.WithRequest(ctx, "Post", url))

	err := fasthttp.Do(req, resp)
	if err != nil {
		return http.ClientResponse{}, err
	}

	respBody, err := getBodyEvenIfCompressed(resp)

	return http.ClientResponse{
		StatusCode: resp.StatusCode(),
		Body:       respBody,
	}, err
}

// Put executes a PUT request calling the given URL, using the given headers and body.
func (a ClientAdapter) Put(ctx context.Context, url string, body []byte, headers []http.Header) (http.ClientResponse, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(url)
	req.Header.SetMethod(fasthttp.MethodPut)
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("Accept", "application/json")
	req.Header.SetContentType("application/json")

	for _, header := range headers {
		req.Header.Set(header.Key, header.Value)
	}

	req.SetBody(body)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	defer a.BenckmarkIntegration.Took(benchmark.WithRequest(ctx, "Put", url))

	err := fasthttp.Do(req, resp)
	if err != nil {
		return http.ClientResponse{}, err
	}

	respBody, err := getBodyEvenIfCompressed(resp)

	return http.ClientResponse{
		StatusCode: resp.StatusCode(),
		Body:       respBody,
	}, err
}
