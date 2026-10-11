package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/code-corhuila/multi-tour-worker-runtime/config"
	"github.com/code-corhuila/multi-tour-worker-runtime/correlation"
	"github.com/code-corhuila/multi-tour-worker-runtime/httpclient"

	relayhttp "github.com/code-corhuila/multi-tour-worker-event-relay/internal/adapter/out/http"
	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/port/out"
)

const (
	token      = "s3cret-service-token"
	outboxPath = "/api/v1/security/internal/outbox"
	eventID    = "00000000-0000-4000-8000-000000001001"
	tenantID   = "00000000-0000-4000-8000-000000002002"
)

// call is what a fake API saw.
type call struct {
	method, path, query string
	header              http.Header
	body                []byte
}

func fakeAPI(t *testing.T, status int, answer string) (*httptest.Server, *call) {
	t.Helper()
	seen := &call{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.method, seen.path, seen.query, seen.header = r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Clone()
		seen.body, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(answer))
	}))
	t.Cleanup(srv.Close)
	return srv, seen
}

// newClient never really sleeps between attempts.
func newClient(url string) *httpclient.Client {
	return httpclient.New(url, config.Secret(token),
		httpclient.WithSleeper(func(context.Context, time.Duration) error { return nil }))
}

func ctx() context.Context { return correlation.WithID(context.Background(), "run-1") }

func TestOutbox_lists_the_first_page_of_pending_events(t *testing.T) {
	page := `{"data":[{"eventId":"` + eventID + `","eventType":"TenantCreated","eventVersion":2,
		"tenantId":"` + tenantID + `","aggregateId":"` + tenantID + `","payload":{"identifier":"acme"},
		"occurredAt":"2026-10-11T10:00:00Z","status":"PENDING","attempts":1}],
		"meta":{"page":1,"limit":50,"total":1,"totalPages":1}}`
	srv, seen := fakeAPI(t, 200, page)

	events, err := relayhttp.NewOutbox(newClient(srv.URL), outboxPath).ListPending(ctx(), 1, 50)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen.method != "GET" || seen.path != outboxPath || seen.query != "limit=50&page=1&status=PENDING" {
		t.Errorf("request = %s %s?%s", seen.method, seen.path, seen.query)
	}
	if seen.header.Get("Authorization") != "Bearer "+token || seen.header.Get(correlation.Header) != "run-1" {
		t.Errorf("headers = %v", seen.header)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	e := events[0]
	if e.ID != eventID || e.Type != "TenantCreated" || e.Version != 2 || e.TenantID != tenantID || e.AggregateID != tenantID {
		t.Errorf("event = %+v", e)
	}
	if string(e.Payload) != `{"identifier":"acme"}` || !e.OccurredAt.Equal(time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)) {
		t.Errorf("payload = %s, occurredAt = %s", e.Payload, e.OccurredAt)
	}
}

func TestOutbox_marks_an_event_published(t *testing.T) {
	srv, seen := fakeAPI(t, 200, `{"eventId":"`+eventID+`","status":"PUBLISHED"}`)

	err := relayhttp.NewOutbox(newClient(srv.URL), outboxPath).MarkPublished(ctx(), eventID)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen.method != "POST" || seen.path != outboxPath+"/"+eventID+"/published" {
		t.Errorf("request = %s %s", seen.method, seen.path)
	}
	if seen.header.Get("Authorization") != "Bearer "+token {
		t.Errorf("Authorization = %q", seen.header.Get("Authorization"))
	}
	// a repeat answers 200 on the owning API, so the runtime may retry the call
	if seen.header.Get("Idempotency-Key") != eventID+":published" {
		t.Errorf("Idempotency-Key = %q", seen.header.Get("Idempotency-Key"))
	}
}

func TestOutbox_records_a_failed_attempt_with_the_error_text(t *testing.T) {
	srv, seen := fakeAPI(t, 200, `{"eventId":"`+eventID+`","status":"PENDING"}`)

	err := relayhttp.NewOutbox(newClient(srv.URL), outboxPath).RecordFailedAttempt(ctx(), eventID, "security: delivery failed")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen.method != "POST" || seen.path != outboxPath+"/"+eventID+"/attempts" {
		t.Errorf("request = %s %s", seen.method, seen.path)
	}
	if string(seen.body) != `{"error":"security: delivery failed"}` {
		t.Errorf("body = %s", seen.body)
	}
}

func TestSink_delivers_the_envelope_with_the_event_id_as_idempotency_key(t *testing.T) {
	for _, status := range []int{201, 200} {
		srv, seen := fakeAPI(t, status, ``)
		sink := relayhttp.NewSink(map[string]*httpclient.Client{"security": newClient(srv.URL)})

		err := sink.Deliver(ctx(), out.Delivery{Consumer: "security", IdempotencyKey: eventID,
			Path: "/api/v1/security/internal/tenants/" + tenantID + "/events",
			Event: out.Event{ID: eventID, Type: "TenantCreated", Version: 1, TenantID: tenantID, AggregateID: tenantID,
				Payload: json.RawMessage(`{"identifier":"acme"}`), OccurredAt: time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC)}})

		if err != nil {
			t.Fatalf("status %d: unexpected error: %v", status, err)
		}
		if seen.method != "POST" || seen.path != "/api/v1/security/internal/tenants/"+tenantID+"/events" {
			t.Errorf("request = %s %s", seen.method, seen.path)
		}
		if seen.header.Get("Idempotency-Key") != eventID || seen.header.Get("Authorization") != "Bearer "+token {
			t.Errorf("headers = %v", seen.header)
		}
		var body map[string]any
		if err := json.Unmarshal(seen.body, &body); err != nil {
			t.Fatalf("body is not JSON: %s", seen.body)
		}
		if body["eventId"] != eventID || body["eventType"] != "TenantCreated" || body["eventVersion"] != float64(1) ||
			body["tenantId"] != tenantID || body["aggregateId"] != tenantID || body["occurredAt"] != "2026-10-11T10:00:00Z" {
			t.Errorf("envelope = %s", seen.body)
		}
		if payload, _ := body["payload"].(map[string]any); payload["identifier"] != "acme" {
			t.Errorf("payload = %v", body["payload"])
		}
	}
}

func TestSink_an_unknown_consumer_is_an_error(t *testing.T) {
	err := relayhttp.NewSink(map[string]*httpclient.Client{}).Deliver(ctx(), out.Delivery{Consumer: "nobody", Path: "/x"})

	if err == nil {
		t.Fatal("want an error for a consumer without a base URL")
	}
}

func TestAdapters_a_5xx_is_a_typed_failure_without_the_raw_body_or_the_token(t *testing.T) {
	raw := "<html>java.lang.NullPointerException at db-host-7 password=hunter2 " + token + "</html>"
	srv, _ := fakeAPI(t, 500, raw)
	client := newClient(srv.URL)
	sink := relayhttp.NewSink(map[string]*httpclient.Client{"security": client})
	outbox := relayhttp.NewOutbox(client, outboxPath)
	_, listErr := outbox.ListPending(ctx(), 1, 10)
	cases := map[string]error{
		"list":    listErr,
		"mark":    outbox.MarkPublished(ctx(), eventID),
		"attempt": outbox.RecordFailedAttempt(ctx(), eventID, "x"),
		"deliver": sink.Deliver(ctx(), out.Delivery{Consumer: "security", Path: "/x", IdempotencyKey: eventID}),
	}
	for name, err := range cases {
		var apiErr *httpclient.APIError
		if !errors.As(err, &apiErr) || apiErr.Status != 500 {
			t.Errorf("%s: want *APIError with status 500, got %T %v", name, err, err)
			continue
		}
		for _, leaked := range []string{"NullPointer", "hunter2", token} {
			if strings.Contains(err.Error(), leaked) {
				t.Errorf("%s: error leaks %q: %v", name, leaked, err)
			}
		}
	}
}

func TestAdapters_an_unreachable_service_is_a_transport_failure_without_the_token(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()

	_, err := relayhttp.NewOutbox(newClient(url), outboxPath).ListPending(ctx(), 1, 10)

	var tErr *httpclient.TransportError
	if !errors.As(err, &tErr) {
		t.Fatalf("want *TransportError, got %T %v", err, err)
	}
	if strings.Contains(err.Error(), token) {
		t.Errorf("error leaks the token: %v", err)
	}
}
