package usecase_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/port/out"
	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/usecase"
	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/config"
)

const maxAttempts = 20

// uuid builds a valid version 4 id, because the relay only puts UUIDs in paths and headers.
func uuid(n int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", n) }

var id1, id2, id3 = uuid(1001), uuid(1002), uuid(1003)

// fakeSource plays the owning API: it drops a published event from the pending list and,
// like the real one, marks an event failed on the 20th recorded attempt.
type fakeSource struct {
	pending   []out.Event
	published map[string]bool
	failed    map[string]bool
	attempts  map[string][]string
	markErr   map[string]error
	limits    []int
	pages     []int
}

func newSource(events ...out.Event) *fakeSource {
	return &fakeSource{
		pending:   events,
		published: map[string]bool{},
		failed:    map[string]bool{},
		attempts:  map[string][]string{},
		markErr:   map[string]error{},
	}
}

func (s *fakeSource) ListPending(_ context.Context, page, limit int) ([]out.Event, error) {
	s.pages = append(s.pages, page)
	s.limits = append(s.limits, limit)
	if limit > len(s.pending) {
		limit = len(s.pending)
	}
	return append([]out.Event(nil), s.pending[:limit]...), nil
}

func (s *fakeSource) MarkPublished(_ context.Context, id string) error {
	if err := s.markErr[id]; err != nil {
		delete(s.markErr, id)
		return err
	}
	s.published[id] = true
	s.drop(id)
	return nil
}

func (s *fakeSource) RecordFailedAttempt(_ context.Context, id, message string) (bool, error) {
	s.attempts[id] = append(s.attempts[id], message)
	if len(s.attempts[id]) >= maxAttempts {
		s.failed[id] = true
		s.drop(id)
	}
	return s.failed[id], nil
}

func (s *fakeSource) drop(id string) {
	for i, e := range s.pending {
		if e.ID == id {
			s.pending = append(s.pending[:i:i], s.pending[i+1:]...)
			return
		}
	}
}

type fakeSink struct {
	mu         sync.Mutex
	deliveries []out.Delivery
	failFor    func(out.Delivery) error
}

func (k *fakeSink) Deliver(_ context.Context, d out.Delivery) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.deliveries = append(k.deliveries, d)
	if k.failFor != nil {
		return k.failFor(d)
	}
	return nil
}

func (k *fakeSink) to(consumer string) []out.Delivery {
	var found []out.Delivery
	for _, d := range k.deliveries {
		if d.Consumer == consumer {
			found = append(found, d)
		}
	}
	return found
}

func event(id, eventType string) out.Event {
	return out.Event{ID: id, Type: eventType, Version: 1, TenantID: id, AggregateID: id}
}

func table(t *testing.T, routes map[string][]out.Route) *config.RoutingTable {
	t.Helper()
	rt, err := config.NewRoutingTable(routes)
	if err != nil {
		t.Fatalf("routing table: %v", err)
	}
	return rt
}

func twoConsumers(t *testing.T) *config.RoutingTable {
	path := "/internal/tenants/{tenantId}/events"
	both := []out.Route{{Consumer: "a", Path: path}, {Consumer: "b", Path: path}}
	return table(t, map[string][]out.Route{
		"TenantCreated":          {{Consumer: "a", Path: path}},
		"TenantInactivated":      both,
		"TenantReactivated":      both,
		"AccountSessionsRevoked": both,
	})
}

func relay(src *fakeSource, rt usecase.Router, sink *fakeSink) *usecase.RelayEvents {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return usecase.NewRelayEvents([]usecase.Publisher{{Name: "security", Source: src}}, rt, sink, log)
}

func TestA_reads_pending_pages_within_the_run(t *testing.T) {
	var events []out.Event
	for i := 0; i < 250; i++ {
		events = append(events, event(uuid(i), "TenantCreated"))
	}
	src, sink := newSource(events...), &fakeSink{}

	processed, failed := relay(src, twoConsumers(t), sink).Run(context.Background(), 120)

	if processed != 120 || failed != 0 {
		t.Fatalf("processed=%d failed=%d, want 120 and 0", processed, failed)
	}
	if len(src.published) != 120 || len(src.pending) != 130 {
		t.Fatalf("published=%d pending=%d, want 120 and 130", len(src.published), len(src.pending))
	}
	if fmt.Sprint(src.limits) != "[100 20]" {
		t.Fatalf("page limits %v, want [100 20]: pages of at most 100 within the batch", src.limits)
	}
	for _, p := range src.pages {
		if p != 1 {
			t.Fatalf("read page %d, want always page 1 (published events leave the list)", p)
		}
	}
}

func TestA_event_marked_only_after_all_consumers_accept(t *testing.T) {
	path := "/internal/tenants/{tenantId}/events"
	rt := table(t, map[string][]out.Route{"TenantInactivated": {{Consumer: "a", Path: path}, {Consumer: "b", Path: path}}})
	src := newSource(event(id1, "TenantInactivated"))
	sink := &fakeSink{failFor: func(d out.Delivery) error {
		if d.Consumer == "b" {
			return errors.New("b answered 503")
		}
		return nil
	}}

	processed, failed := relay(src, rt, sink).Run(context.Background(), 10)

	if processed != 0 || failed != 1 {
		t.Fatalf("processed=%d failed=%d, want 0 and 1", processed, failed)
	}
	if src.published[id1] {
		t.Fatal("marked published although consumer b refused it")
	}
	sink.failFor = nil
	if processed, _ = relay(src, rt, sink).Run(context.Background(), 10); processed != 1 || !src.published[id1] {
		t.Fatalf("processed=%d published=%v, want the event published once both accept", processed, src.published)
	}
}

func TestC_repeat_delivery_sends_same_idempotency_key(t *testing.T) {
	src := newSource(event(id1, "TenantCreated"))
	src.markErr[id1] = errors.New("mark lost")
	sink := &fakeSink{}
	job := relay(src, twoConsumers(t), sink)

	if _, failed := job.Run(context.Background(), 10); failed != 1 {
		t.Fatalf("failed=%d, want 1 when marking fails", failed)
	}
	job.Run(context.Background(), 10)

	got := sink.to("a")
	if len(got) != 2 {
		t.Fatalf("%d deliveries, want 2 (at least once)", len(got))
	}
	for _, d := range got {
		if d.IdempotencyKey != id1 {
			t.Fatalf("idempotency key %q, want the event id", d.IdempotencyKey)
		}
	}
	if got[0].Path != "/internal/tenants/"+id1+"/events" {
		t.Fatalf("path %q, want the tenant id in it", got[0].Path)
	}
}

func TestA_routing_table_sends_broadcast_types_to_every_service(t *testing.T) {
	broadcast := []string{"TenantInactivated", "TenantReactivated", "AccountSessionsRevoked"}
	var events []out.Event
	for i, eventType := range broadcast {
		events = append(events, event(uuid(10+i), eventType))
	}
	src, sink := newSource(events...), &fakeSink{}

	relay(src, twoConsumers(t), sink).Run(context.Background(), 10)

	for _, consumer := range []string{"a", "b"} {
		if n := len(sink.to(consumer)); n != len(broadcast) {
			t.Fatalf("consumer %s got %d events, want %d", consumer, n, len(broadcast))
		}
	}
}

func TestA_twentieth_failure_marks_the_event_failed(t *testing.T) {
	src := newSource(event(id1, "TenantCreated"))
	sink := &fakeSink{failFor: func(out.Delivery) error { return errors.New("down\nat host=10.0.0.5\tport=80") }}
	job := relay(src, twoConsumers(t), sink)

	for run := 1; run <= maxAttempts; run++ {
		if src.failed[id1] {
			t.Fatalf("failed before attempt %d", maxAttempts)
		}
		job.Run(context.Background(), 10)
	}

	if !src.failed[id1] || len(src.attempts[id1]) != maxAttempts {
		t.Fatalf("failed=%v attempts=%d, want failed after %d recorded attempts", src.failed[id1], len(src.attempts[id1]), maxAttempts)
	}
	if msg := src.attempts[id1][0]; strings.ContainsAny(msg, "\n\t\r") || msg == "" || len(msg) > 1000 || strings.Contains(msg, "10.0.0.5") {
		t.Fatalf("error %q is not a sanitized single line of at most 1000 characters without the raw detail", msg)
	}
	if _, failed := job.Run(context.Background(), 10); failed != 0 || len(sink.deliveries) != maxAttempts {
		t.Fatal("a failed event must not be delivered again")
	}
}

// alertLines returns the JSON log lines that carry an alert field.
func alertLines(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var found []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if _, ok := entry["alert"]; ok {
			found = append(found, entry)
		}
	}
	return found
}

func TestA_failed_event_raises_an_alert_without_the_payload(t *testing.T) {
	e := event(id1, "TenantCreated")
	e.Payload = json.RawMessage(`{"secret":"payload-must-not-be-logged"}`)
	src := newSource(e)
	sink := &fakeSink{failFor: func(out.Delivery) error { return errors.New("down") }}
	var logs bytes.Buffer
	job := usecase.NewRelayEvents([]usecase.Publisher{{Name: "security", Source: src}}, twoConsumers(t), sink,
		slog.New(slog.NewJSONHandler(&logs, nil)))

	for run := 1; run < maxAttempts; run++ {
		job.Run(context.Background(), 10)
	}
	if got := alertLines(t, &logs); len(got) != 0 {
		t.Fatalf("an alert before the event failed: %v", got)
	}
	job.Run(context.Background(), 10)

	alerts := alertLines(t, &logs)
	if len(alerts) != 1 || alerts[0]["alert"] != "event_failed" || alerts[0]["level"] != "ERROR" ||
		alerts[0]["eventId"] != id1 || alerts[0]["eventType"] != "TenantCreated" || alerts[0]["publisher"] != "security" {
		t.Fatalf("alerts = %v, want one ERROR event_failed with the event id, type and publisher", alerts)
	}
	if strings.Contains(logs.String(), "payload-must-not-be-logged") {
		t.Fatal("the payload reached the log")
	}
}

func TestA_event_pending_over_five_minutes_raises_an_alert(t *testing.T) {
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	old, recent := event(id1, "TenantCreated"), event(id2, "TenantCreated")
	old.OccurredAt, recent.OccurredAt = now.Add(-6*time.Minute), now.Add(-2*time.Minute)
	src, sink := newSource(old, recent), &fakeSink{}
	var logs bytes.Buffer
	job := usecase.NewRelayEvents([]usecase.Publisher{{Name: "security", Source: src}}, twoConsumers(t), sink,
		slog.New(slog.NewJSONHandler(&logs, nil))).WithClock(func() time.Time { return now })

	job.Run(context.Background(), 10)

	alerts := alertLines(t, &logs)
	if len(alerts) != 1 || alerts[0]["alert"] != "event_pending_too_long" || alerts[0]["level"] != "WARN" ||
		alerts[0]["eventId"] != id1 || alerts[0]["publisher"] != "security" {
		t.Fatalf("alerts = %v, want one WARN event_pending_too_long for the old event only", alerts)
	}
}

func TestC_a_tenant_id_that_is_not_a_uuid_never_becomes_a_path(t *testing.T) {
	for _, bad := range []string{"..", "a/b", "x?y", "%2e", "", id1 + "/../x"} {
		e := event(id1, "TenantCreated")
		e.TenantID = bad
		src, sink := newSource(e), &fakeSink{}

		processed, failed := relay(src, twoConsumers(t), sink).Run(context.Background(), 10)

		if processed != 0 || failed != 1 || len(sink.deliveries) != 0 || src.published[id1] {
			t.Errorf("tenantId %q: processed=%d failed=%d deliveries=%d, want a counted failure and no delivery", bad, processed, failed, len(sink.deliveries))
		}
		if len(src.attempts[id1]) != 1 {
			t.Errorf("tenantId %q: attempts=%v, want one recorded", bad, src.attempts[id1])
		}
	}
}

func TestC_an_event_id_that_is_not_a_uuid_is_counted_and_never_addressed(t *testing.T) {
	for _, bad := range []string{"..", "a/b", "x?y", "%2e", "", "short"} {
		src, sink := newSource(event(bad, "TenantCreated")), &fakeSink{}

		processed, failed := relay(src, twoConsumers(t), sink).Run(context.Background(), 10)

		if processed != 0 || failed != 1 || len(sink.deliveries) != 0 {
			t.Errorf("id %q: processed=%d failed=%d deliveries=%d, want a counted failure and no delivery", bad, processed, failed, len(sink.deliveries))
		}
		if len(src.published) != 0 || len(src.attempts) != 0 {
			t.Errorf("id %q: the source must not be addressed with it, got published=%v attempts=%v", bad, src.published, src.attempts)
		}
	}
}

func TestC_the_logged_error_carries_none_of_the_raw_detail(t *testing.T) {
	var logged bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logged, nil))
	src := newSource(event(id1, "TenantCreated"))
	sink := &fakeSink{failFor: func(out.Delivery) error { return errors.New("dial host=10.0.0.5 refused") }}

	usecase.NewRelayEvents([]usecase.Publisher{{Name: "security", Source: src}}, twoConsumers(t), sink, log).Run(context.Background(), 10)

	if !strings.Contains(logged.String(), "delivery failed") || strings.Contains(logged.String(), "10.0.0.5") {
		t.Fatalf("log %q must hold the sanitized message and not the raw detail", logged.String())
	}
}

func TestA_an_event_that_stays_pending_is_read_once_and_the_drain_ends(t *testing.T) {
	src := newSource(event(id1, "TenantCreated"))
	sink := &fakeSink{failFor: func(out.Delivery) error { return errors.New("refused") }}

	processed, failed := relay(src, twoConsumers(t), sink).Run(context.Background(), 10)

	if processed != 0 || failed != 1 || len(sink.deliveries) != 1 {
		t.Fatalf("processed=%d failed=%d deliveries=%d, want 0, 1 and 1", processed, failed, len(sink.deliveries))
	}
	if len(src.pages) != 2 {
		t.Fatalf("%d reads, want 2: the event, then a page of events already seen", len(src.pages))
	}
}

func TestA_one_failing_event_does_not_stop_the_batch(t *testing.T) {
	src := newSource(event(id1, "TenantCreated"), event(id2, "NoSuchEvent"), event(id3, "TenantCreated"))
	sink := &fakeSink{failFor: func(d out.Delivery) error {
		if d.Event.ID == id1 {
			return errors.New("refused")
		}
		return nil
	}}

	processed, failed := relay(src, twoConsumers(t), sink).Run(context.Background(), 10)

	if processed != 1 || failed != 2 {
		t.Fatalf("processed=%d failed=%d, want 1 and 2 (refused and unknown type counted)", processed, failed)
	}
	if !src.published[id3] || src.published[id1] || src.published[id2] {
		t.Fatalf("published=%v, want only e3", src.published)
	}
	if len(src.attempts[id1]) != 1 || len(src.attempts[id2]) != 1 {
		t.Fatalf("attempts=%v, want one recorded for each failure", src.attempts)
	}
}
