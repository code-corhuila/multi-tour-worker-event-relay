package usecase_test

import (
	"context"
	"testing"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/config"
)

func defaultTable(t *testing.T) *config.RoutingTable {
	t.Helper()
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	return rt
}

// RN-WRK7-15: an event nobody consumes yet is marked published, so it does not pile up in the outbox, and nothing is sent.
func TestA_event_with_no_consumer_is_marked_published(t *testing.T) {
	for _, eventType := range []string{"AccountTemporaryCredentialIssued", "SalidaMarkedPendienteDeRegularizacion"} {
		src, sink := newSource(event(id1, eventType)), &fakeSink{}

		processed, failed := relay(src, defaultTable(t), sink).Run(context.Background(), 10)

		if processed != 1 || failed != 0 || !src.published[id1] || len(sink.deliveries) != 0 {
			t.Errorf("%s: processed=%d failed=%d published=%v deliveries=%d, want published and nothing sent",
				eventType, processed, failed, src.published[id1], len(sink.deliveries))
		}
		if len(src.attempts[id1]) != 0 {
			t.Errorf("%s: attempts=%v, want none: no alert for an event nobody consumes", eventType, src.attempts[id1])
		}
	}
}

// Relay spec 007 (AC-RL-07.2): with the real table a verified account is delivered to commercial-reservation and to the audit, and only then
// marked published.
func TestA_a_verified_account_is_delivered_to_both_consumers_before_it_is_marked_published(t *testing.T) {
	e := event(id1, "AccountEmailVerified")
	e.TenantID = id2
	src, sink := newSource(e), &fakeSink{}

	processed, failed := relay(src, defaultTable(t), sink).Run(context.Background(), 10)

	if processed != 1 || failed != 0 || !src.published[id1] {
		t.Fatalf("processed=%d failed=%d published=%v, want 1, 0 and published", processed, failed, src.published[id1])
	}
	if len(sink.to("commercial-reservation")) != 1 || len(sink.to("identity-audit")) != 1 {
		t.Errorf("deliveries=%v, want one to commercial-reservation and one to identity-audit", sink.deliveries)
	}
}

// RN-WRK7-11 and -12 with the real table: the same event goes to one path or the other by whether it carries a tenant.
func TestA_an_audited_event_takes_the_path_that_fits_its_tenant(t *testing.T) {
	withTenant := event(id1, "SpecialPermissionGranted")
	withTenant.TenantID = id2
	platform := event(id3, "AccountBlocked")
	platform.TenantID = ""
	src, sink := newSource(withTenant, platform), &fakeSink{}

	processed, failed := relay(src, defaultTable(t), sink).Run(context.Background(), 10)

	if processed != 2 || failed != 0 {
		t.Fatalf("processed=%d failed=%d, want 2 and 0", processed, failed)
	}
	if len(sink.deliveries) != 2 || len(sink.to("identity-audit")) != 2 {
		t.Fatalf("deliveries=%v, want exactly one per event and all to identity-audit", sink.deliveries)
	}
	got := map[string]string{}
	for _, d := range sink.to("identity-audit") {
		got[d.IdempotencyKey] = d.Path
	}
	if got[id1] != "/api/v1/identity-audit/internal/tenants/"+id2+"/events" {
		t.Errorf("event with a tenant went to %q", got[id1])
	}
	if got[id3] != "/api/v1/identity-audit/internal/events" {
		t.Errorf("event with no tenant went to %q", got[id3])
	}
}
