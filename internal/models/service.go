package models

import (
	"context"
)

type HTTP interface {
	Get(ctx context.Context, api string, params map[string]interface{}) (*Response, error)
	Post(ctx context.Context, api string, params map[string]interface{}, body []byte) (*Response, error)
	Put(ctx context.Context, api string, params map[string]interface{}, body []byte) (*Response, error)
	Delete(ctx context.Context, api string, body []byte) (*Response, error)
	Patch(ctx context.Context, api string, params map[string]interface{}, body []byte) (*Response, error)

	GetWithHeaders(ctx context.Context, api string, params map[string]interface{}, headers map[string]string) (*Response, error)
	PostWithHeaders(ctx context.Context, api string, params map[string]interface{}, body []byte, headers map[string]string) (*Response, error)
	PutWithHeaders(ctx context.Context, api string, params map[string]interface{}, body []byte, headers map[string]string) (*Response, error)
	DeleteWithHeaders(ctx context.Context, api string, body []byte, headers map[string]string) (*Response, error)
	PatchWithHeaders(ctx context.Context, api string, params map[string]interface{}, body []byte, headers map[string]string) (*Response, error)

	Bind(resp []byte, i interface{}) error
	BindStrict(resp []byte, i interface{}) error

	// PropagateHeaders is used to specify the header keys that needs to be propagated through context
	// By default the headers: True-Client-IP, X-Kroger-Channel, X-Kroger-Location, X-Authenticated-UserId, X-Kroger-Tenant
	// are propagated.
	PropagateHeaders(headers ...string)

	// SetSurgeProtectorOptions sets the configuration for the surge protector, the default configuration is :-
	// surge protection is enabled, the heartbeat URL is /.well-known/heartbeat, retry frequency is 5 seconds.
	// The surge protector ensures that the HTTP client does not bombard a downstream service that is down,
	// it returns a 500 right away, until the service is back up again. It figures out if the service
	// is back up again by asynchronously making request to the heartbeat API until its up again
	SetSurgeProtectorOptions(isEnabled bool, customHeartbeatURL string, retryFrequencySeconds int)
}

type Response struct {
	Body       []byte
	StatusCode int
}
