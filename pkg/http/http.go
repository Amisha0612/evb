package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/Amisha0612/evb/internal/models"
)

type ClientWrapper struct {
	baseURL string
	client  *http.Client
}

func NewHTTPClient(baseURL string, client *http.Client) *ClientWrapper {
	return &ClientWrapper{
		baseURL: baseURL,
		client:  client,
	}
}

func (h *ClientWrapper) buildURL(api string, params map[string]interface{}) string {
	u, _ := url.Parse(h.baseURL + api)
	q := u.Query()
	for k, v := range params {
		q.Set(k, toString(v))
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (h *ClientWrapper) doRequest(ctx context.Context, method, api string, params map[string]interface{}, body []byte, headers map[string]string) (*models.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, h.buildURL(api, params), bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	out := &models.Response{
		Body:       respBody,
		StatusCode: resp.StatusCode,
	}

	// The response is returned even on failure so callers can inspect the body.
	if resp.StatusCode >= http.StatusBadRequest {
		return out, &StatusError{Method: method, URL: req.URL.Redacted(), StatusCode: resp.StatusCode}
	}

	return out, nil
}

// StatusError is returned when the server answers with a 4xx or 5xx status.
type StatusError struct {
	Method     string
	URL        string
	StatusCode int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d %s", e.Method, e.URL, e.StatusCode, http.StatusText(e.StatusCode))
}

// Retryable reports whether sending the same request again can succeed: server
// errors, timeouts and rate limiting can, while other client errors cannot.
func (e *StatusError) Retryable() bool {
	switch {
	case e.StatusCode >= http.StatusInternalServerError:
		return true
	case e.StatusCode == http.StatusRequestTimeout, e.StatusCode == http.StatusTooManyRequests:
		return true
	default:
		return false
	}
}

func (h *ClientWrapper) GetWithHeaders(ctx context.Context, api string, params map[string]interface{}, headers map[string]string) (*models.Response, error) {
	return h.doRequest(ctx, http.MethodGet, api, params, nil, headers)
}

func (h *ClientWrapper) PostWithHeaders(ctx context.Context, api string, params map[string]interface{}, body []byte, headers map[string]string) (*models.Response, error) {
	return h.doRequest(ctx, http.MethodPost, api, params, body, headers)
}

func (h *ClientWrapper) PutWithHeaders(ctx context.Context, api string, params map[string]interface{}, body []byte, headers map[string]string) (*models.Response, error) {
	return h.doRequest(ctx, http.MethodPut, api, params, body, headers)
}

func (h *ClientWrapper) DeleteWithHeaders(ctx context.Context, api string, body []byte, headers map[string]string) (*models.Response, error) {
	return h.doRequest(ctx, http.MethodDelete, api, nil, body, headers)
}

func (h *ClientWrapper) PatchWithHeaders(ctx context.Context, api string, params map[string]interface{}, body []byte, headers map[string]string) (*models.Response, error) {
	return h.doRequest(ctx, http.MethodPatch, api, params, body, headers)
}

// safe string conversion for query params
func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	default:
		return ""
	}
}
