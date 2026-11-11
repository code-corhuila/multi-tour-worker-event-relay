package usecase_test

import (
	"context"
	"strings"
	"testing"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/port/out"
)

const (
	withTenantPath    = "/internal/tenants/{tenantId}/events"
	withoutTenantPath = "/internal/events"
)

// auditRoute is a consumer that has a path for the events that belong to no tenant (ADR-004 D4).
func auditRoute() out.Route {
	return out.Route{Consumer: "audit", Path: withTenantPath, NoTenantPath: withoutTenantPath}
}

func eventWithTenant(id, tenantID string) out.Event {
	e := event(id, "AccountBlocked")
	e.TenantID = tenantID
	return e
}

func TestA_event_with_a_tenant_goes_to_the_path_with_the_tenant(t *testing.T) {
	rt := table(t, map[string][]out.Route{"AccountBlocked": {auditRoute()}})
	src, sink := newSource(eventWithTenant(id1, id2)), &fakeSink{}

	processed, failed := relay(src, rt, sink).Run(context.Background(), 10)

	if processed != 1 || failed != 0 || !src.published[id1] {
		t.Fatalf("processed=%d failed=%d published=%v, want the event published", processed, failed, src.published)
	}
	if len(sink.deliveries) != 1 || sink.deliveries[0].Path != "/internal/tenants/"+id2+"/events" {
		t.Fatalf("deliveries=%v, want one to the path of the tenant", sink.deliveries)
	}
}

func TestA_event_with_no_tenant_goes_to_the_path_without_the_tenant(t *testing.T) {
	rt := table(t, map[string][]out.Route{"AccountBlocked": {auditRoute()}})
	src, sink := newSource(eventWithTenant(id1, "")), &fakeSink{}

	processed, failed := relay(src, rt, sink).Run(context.Background(), 10)

	if processed != 1 || failed != 0 || !src.published[id1] {
		t.Fatalf("processed=%d failed=%d published=%v, want the event published", processed, failed, src.published)
	}
	if len(sink.deliveries) != 1 || sink.deliveries[0].Path != withoutTenantPath || sink.deliveries[0].IdempotencyKey != id1 {
		t.Fatalf("deliveries=%v, want one to %s with the event id as the key", sink.deliveries, withoutTenantPath)
	}
}

func TestA_event_with_no_tenant_fails_on_a_route_with_no_path_without_tenant(t *testing.T) {
	rt := table(t, map[string][]out.Route{"AccountBlocked": {{Consumer: "security", Path: withTenantPath}}})
	src, sink := newSource(eventWithTenant(id1, "")), &fakeSink{}

	processed, failed := relay(src, rt, sink).Run(context.Background(), 10)

	if processed != 0 || failed != 1 || src.published[id1] || len(sink.deliveries) != 0 {
		t.Fatalf("processed=%d failed=%d deliveries=%d, want a counted failure and no delivery", processed, failed, len(sink.deliveries))
	}
	if len(src.attempts[id1]) != 1 || !strings.Contains(src.attempts[id1][0], "invalid tenantId") {
		t.Fatalf("attempts=%v, want one that says the tenant is invalid", src.attempts[id1])
	}
}

func TestA_event_with_a_malformed_tenant_fails_and_is_not_sent_without_tenant(t *testing.T) {
	rt := table(t, map[string][]out.Route{"AccountBlocked": {auditRoute()}})
	for _, bad := range []string{"not-a-uuid", " ", "..", id2 + "/../x", "x?y"} {
		src, sink := newSource(eventWithTenant(id1, bad)), &fakeSink{}

		processed, failed := relay(src, rt, sink).Run(context.Background(), 10)

		if processed != 0 || failed != 1 || src.published[id1] || len(sink.deliveries) != 0 {
			t.Errorf("tenantId %q: processed=%d failed=%d deliveries=%v, want a counted failure and nothing sent, not even without tenant",
				bad, processed, failed, sink.deliveries)
		}
		if len(src.attempts[id1]) != 1 || !strings.Contains(src.attempts[id1][0], "invalid tenantId") {
			t.Errorf("tenantId %q: attempts=%v, want one that says the tenant is invalid", bad, src.attempts[id1])
		}
	}
}
