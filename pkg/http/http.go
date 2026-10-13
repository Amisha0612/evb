package http

import (
	"bytes"
	"context"
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

	return &models.Response{
		Body:       respBody,
		StatusCode: resp.StatusCode,
	}, nil
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
