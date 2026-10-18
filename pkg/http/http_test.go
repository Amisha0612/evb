package http

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestWrapper(t *testing.T, handler http.HandlerFunc) *ClientWrapper {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return NewHTTPClient(srv.URL, srv.Client())
}

func TestRequestsCarryMethodBodyHeadersAndQuery(t *testing.T) {
	var gotMethod, gotQuery, gotHeader, gotBody string

	w := newTestWrapper(t, func(rw http.ResponseWriter, r *http.Request) {
		gotMethod, gotQuery, gotHeader = r.Method, r.URL.RawQuery, r.Header.Get("X-Test")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)

		rw.WriteHeader(http.StatusOK)
	})

	resp, err := w.PostWithHeaders(context.Background(), "/hook", map[string]interface{}{"a": "1"}, []byte(`{"k":"v"}`), map[string]string{"X-Test": "yes"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d", resp.StatusCode)
	}

	if gotMethod != http.MethodPost || gotQuery != "a=1" || gotHeader != "yes" || gotBody != `{"k":"v"}` {
		t.Errorf("request not sent as expected: method=%s query=%s header=%s body=%s", gotMethod, gotQuery, gotHeader, gotBody)
	}
}

func TestEveryVerbUsesItsMethod(t *testing.T) {
	var got string

	w := newTestWrapper(t, func(_ http.ResponseWriter, r *http.Request) { got = r.Method })
	ctx := context.Background()

	calls := map[string]func() error{
		http.MethodGet:    func() error { _, err := w.GetWithHeaders(ctx, "/", nil, nil); return err },
		http.MethodPost:   func() error { _, err := w.PostWithHeaders(ctx, "/", nil, nil, nil); return err },
		http.MethodPut:    func() error { _, err := w.PutWithHeaders(ctx, "/", nil, nil, nil); return err },
		http.MethodPatch:  func() error { _, err := w.PatchWithHeaders(ctx, "/", nil, nil, nil); return err },
		http.MethodDelete: func() error { _, err := w.DeleteWithHeaders(ctx, "/", nil, nil); return err },
	}

	for method, call := range calls {
		if err := call(); err != nil {
			t.Errorf("%s: unexpected error %v", method, err)
		}

		if got != method {
			t.Errorf("sent %q, want %q", got, method)
		}
	}
}

// A sink that answers 4xx/5xx has not accepted the event, so that must be an error.
func TestErrorStatusesAreErrors(t *testing.T) {
	testCases := []struct {
		status    int
		wantError bool
		permanent bool
	}{
		{http.StatusOK, false, false},
		{http.StatusCreated, false, false},
		{http.StatusNoContent, false, false},
		{http.StatusBadRequest, true, true},
		{http.StatusUnauthorized, true, true},
		{http.StatusNotFound, true, true},
		{http.StatusRequestTimeout, true, false},
		{http.StatusTooManyRequests, true, false},
		{http.StatusInternalServerError, true, false},
		{http.StatusBadGateway, true, false},
		{http.StatusServiceUnavailable, true, false},
	}

	for _, tc := range testCases {
		w := newTestWrapper(t, func(rw http.ResponseWriter, _ *http.Request) {
			rw.WriteHeader(tc.status)
			_, _ = rw.Write([]byte("body"))
		})

		resp, err := w.GetWithHeaders(context.Background(), "/", nil, nil)

		if (err != nil) != tc.wantError {
			t.Errorf("status %d: error = %v, want error=%v", tc.status, err, tc.wantError)
			continue
		}

		if resp == nil || resp.StatusCode != tc.status {
			t.Errorf("status %d: the response must still be returned, got %+v", tc.status, resp)
		}

		if err != nil {
			var se *StatusError
			if !errors.As(err, &se) || se.StatusCode != tc.status {
				t.Errorf("status %d: expected a *StatusError, got %T %v", tc.status, err, err)
				continue
			}

			if se.Retryable() == tc.permanent {
				t.Errorf("status %d: Retryable() = %v, want %v", tc.status, se.Retryable(), !tc.permanent)
			}
		}
	}
}

func TestUnreachableServerIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()

	w := NewHTTPClient(url, http.DefaultClient)

	if _, err := w.GetWithHeaders(context.Background(), "/", nil, nil); err == nil {
		t.Error("expected a connection error")
	}
}

func TestCancelledContextStopsTheRequest(t *testing.T) {
	w := newTestWrapper(t, func(rw http.ResponseWriter, _ *http.Request) { rw.WriteHeader(http.StatusOK) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := w.GetWithHeaders(ctx, "/", nil, nil); err == nil {
		t.Error("expected an error for a cancelled context")
	}
}

func TestToStringOnlyKeepsStrings(t *testing.T) {
	if toString("a") != "a" || toString(1) != "" || toString(nil) != "" {
		t.Errorf("toString: %q %q %q", toString("a"), toString(1), toString(nil))
	}
}
