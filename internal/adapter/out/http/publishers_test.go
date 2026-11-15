package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/code-corhuila/multi-tour-worker-runtime/httpclient"

	relayhttp "github.com/code-corhuila/multi-tour-worker-event-relay/internal/adapter/out/http"
)

// Relay spec 007 (AC-RL-07.3): the outbox of commercial-reservation is read and the one of cash-reporting (G-RL7-1) are read as publishers, next to the other three.
func TestDefaultPublishers_read_the_five_outboxes_each_by_its_own_route(t *testing.T) {
	paths := map[string]string{}
	clients := map[string]*httpclient.Client{}
	for _, name := range []string{"identity-audit", "security", "operations-cost", "commercial-reservation", "cash-reporting"} {
		name := name
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			paths[name] = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[],"meta":{"page":1,"limit":100,"total":0,"totalPages":1}}`))
		}))
		t.Cleanup(srv.Close)
		clients[name] = newClient(srv.URL)
	}

	publishers, err := relayhttp.DefaultPublishers(clients)

	if err != nil {
		t.Fatalf("DefaultPublishers: %v", err)
	}
	var names []string
	for _, p := range publishers {
		names = append(names, p.Name)
		if _, err := p.Source.ListPending(ctx(), 1, 10); err != nil {
			t.Errorf("%s: %v", p.Name, err)
		}
	}
	if got, want := len(names), 5; got != want {
		t.Fatalf("publishers = %v, want the five outboxes", names)
	}
	for _, name := range []string{"identity-audit", "security", "operations-cost", "commercial-reservation", "cash-reporting"} {
		if want := "/api/v1/" + name + "/internal/outbox"; paths[name] != want {
			t.Errorf("%s was read at %q, want %q", name, paths[name], want)
		}
	}
}

func TestDefaultPublishers_refuse_a_missing_client_at_start(t *testing.T) {
	if _, err := relayhttp.DefaultPublishers(map[string]*httpclient.Client{"security": nil}); err == nil {
		t.Error("a publisher without a client was accepted, want an error at start")
	}
}
