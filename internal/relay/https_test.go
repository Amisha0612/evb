package relay

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Amisha0612/evb/internal/models"
	evbhttp "github.com/Amisha0612/evb/pkg/http"
)

func newTestHTTPSSink(t *testing.T, method string, handler http.HandlerFunc) *https {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return &https{
		client: evbhttp.NewHTTPClient(srv.URL, srv.Client()),
		id:     "test-https",
		Path:   "/hook",
		Method: method,
	}
}

func TestHTTPSSinkPostsTheEventAsJSON(t *testing.T) {
	var gotMethod string

	var gotEvent models.StitchedEvent

	sink := newTestHTTPSSink(t, "POST", func(rw http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotEvent)

		rw.WriteHeader(http.StatusAccepted)
	})

	if _, err := sink.Transport(context.Background(), &models.StitchedEvent{TxID: 5, Server: "s"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotMethod != http.MethodPost || gotEvent.TxID != 5 || gotEvent.Server != "s" {
		t.Errorf("event not delivered as JSON: method=%s event=%+v", gotMethod, gotEvent)
	}
}

func TestHTTPSSinkClassifiesFailures(t *testing.T) {
	testCases := []struct {
		status    int
		permanent bool
	}{
		{http.StatusBadRequest, true},
		{http.StatusForbidden, true},
		{http.StatusUnprocessableEntity, true},
		{http.StatusTooManyRequests, false},
		{http.StatusInternalServerError, false},
		{http.StatusServiceUnavailable, false},
	}

	for _, tc := range testCases {
		sink := newTestHTTPSSink(t, "POST", func(rw http.ResponseWriter, _ *http.Request) { rw.WriteHeader(tc.status) })

		_, err := sink.Transport(context.Background(), &models.StitchedEvent{})
		if err == nil {
			t.Fatalf("status %d: expected an error", tc.status)
		}

		if isPermanent(err) != tc.permanent {
			t.Errorf("status %d: isPermanent = %v, want %v", tc.status, isPermanent(err), tc.permanent)
		}
	}
}

func TestHTTPSSinkTreatsNetworkErrorsAsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	client := evbhttp.NewHTTPClient(srv.URL, srv.Client())
	srv.Close()

	sink := &https{client: client, id: "down", Path: "/", Method: "POST"}

	_, err := sink.Transport(context.Background(), &models.StitchedEvent{})
	if err == nil || isPermanent(err) {
		t.Errorf("a connection failure must be a retryable error, got %v (permanent=%v)", err, isPermanent(err))
	}
}

func TestHTTPSSinkRejectsUnsupportedMethodPermanently(t *testing.T) {
	sink := &https{id: "bad-method", Method: "TRACE"}

	_, err := sink.Transport(context.Background(), &models.StitchedEvent{})
	if err == nil || !isPermanent(err) {
		t.Errorf("an unsupported method will never work, so it must be permanent: %v", err)
	}
}

func TestPermanentUnwrapsToTheCause(t *testing.T) {
	cause := errors.New("root cause")

	if !errors.Is(permanent(cause), cause) {
		t.Error("permanent() must keep the original error reachable with errors.Is")
	}

	if permanent(nil) != nil {
		t.Error("permanent(nil) must stay nil")
	}
}
