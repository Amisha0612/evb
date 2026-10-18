package models

// Response is the result of an outbound HTTP call made by a relay sink.
type Response struct {
	Body       []byte
	StatusCode int
}
