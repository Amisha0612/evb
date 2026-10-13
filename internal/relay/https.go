package relay

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/d3ltmeshQ/evb/pkg/http"
	"github.com/d3ltmeshQ/evb/pkg/logger"
	"github.com/google/uuid"
	"golang.org/x/oauth2/clientcredentials"
)

type oAuth struct {
	ClientID     string   `yaml:"clientId"`
	ClientSecret string   `yaml:"clientSecret"`
	TokenURL     string   `yaml:"keyProviderURL"`
	Scopes       []string `yaml:"scopes"`
}

type param struct {
	Values map[string]interface{} `yaml:"query"`
}

type https struct {
	client  *http.HttpClientWrapper
	id      string
	URL     string            `yaml:"url"`
	Path    string            `yaml:"path"`
	Method  string            `yaml:"method"`
	Params  param             `yaml:"params"`
	Headers map[string]string `yaml:"headers"`
	Auth    oAuth             `yaml:"auth"`
}

type httpController interface {
	GetID() string
	Transport(context.Context, any) (any, error)
}

func newHTTPS(obj https) httpController {
	conf := &clientcredentials.Config{
		ClientID:     obj.Auth.ClientID,
		ClientSecret: obj.Auth.ClientSecret,
		TokenURL:     obj.Auth.TokenURL,
		Scopes:       obj.Auth.Scopes,
	}

	ctx := context.Background()
	oauthClient := conf.Client(ctx)
	oauthClient.Timeout = time.Second * 5

	httpClient := http.NewHTTPClient(obj.URL, oauthClient)

	return &https{
		client:  httpClient,
		id:      uuid.New().String(),
		URL:     obj.URL,
		Path:    obj.Path,
		Method:  obj.Method,
		Params:  obj.Params,
		Headers: obj.Headers,
		Auth:    obj.Auth,
	}
}

func (h *https) GetID() string {
	return h.id
}

func (h *https) Transport(ctx context.Context, payload any) (any, error) {
	var body []byte
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = b
	}

	switch h.Method {
	case "GET":
		resp, err := h.client.GetWithHeaders(ctx, h.Path, h.Params.Values, h.Headers)
		if err != nil {
			logger.Errorf("HTTP GET request failed: %v", err)
			return nil, err
		}
		logger.Infof("HTTP response: %v", resp)
		return resp, nil
	case "POST":
		resp, err := h.client.PostWithHeaders(ctx, h.Path, h.Params.Values, body, h.Headers)
		if err != nil {
			logger.Errorf("HTTP POST request failed: %v", err)
			return nil, err
		}
		logger.Infof("HTTP response: %v", resp)
		return resp, nil
	case "PATCH":
		resp, err := h.client.PatchWithHeaders(ctx, h.Path, h.Params.Values, body, h.Headers)
		if err != nil {
			logger.Errorf("HTTP PATCH request failed: %v", err)
			return nil, err
		}
		logger.Infof("HTTP response: %v", resp)
		return resp, nil
	case "DELETE":
		resp, err := h.client.DeleteWithHeaders(ctx, h.Path, body, h.Headers)
		if err != nil {
			logger.Errorf("HTTP DELETE request failed: %v", err)
			return nil, err
		}
		logger.Infof("HTTP response: %v", resp)
		return resp, nil
	default:
		return nil, errors.New("unsupported HTTP method: " + h.Method)
	}
}
