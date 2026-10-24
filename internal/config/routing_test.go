package config_test

import (
	"fmt"
	"testing"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/port/out"
	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/config"
)

func TestC_startup_fails_with_an_unknown_event_type_in_the_routing_table(t *testing.T) {
	route := out.Route{Consumer: "security", Path: "/internal/tenants/{tenantId}/events"}
	cases := map[string]map[string][]out.Route{
		"unknown type":      {"TenantExploded": {route}},
		"consumer is empty": {"TenantCreated": {{Path: route.Path}}},
		"path is empty":     {"TenantCreated": {{Consumer: "security"}}},
	}
	for name, routes := range cases {
		if _, err := config.NewRoutingTable(routes); err == nil {
			t.Errorf("%s: the table was accepted, want an error at start", name)
		}
	}
}

func TestDefaultRoutesAreValidAndSendTenantEventsToSecurity(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	for _, eventType := range []string{"TenantCreated", "TenantInactivated", "TenantReactivated"} {
		routes, known := rt.RoutesFor(eventType)
		want := out.Route{Consumer: "security", Path: "/api/v1/security/internal/tenants/{tenantId}/events"}
		if !known || len(routes) != 1 || routes[0] != want {
			t.Errorf("%s: routes=%v known=%v, want %v", eventType, routes, known, want)
		}
	}
}

// The consumers of the table are the ones the worker has a client for: main refuses to start otherwise, so a route
// added here without its client is found at start and not at the first delivery.
func TestC_default_routes_name_only_the_consumers_the_worker_serves(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	if got, want := fmt.Sprint(rt.Consumers()), "[identity-audit security]"; got != want {
		t.Errorf("consumers = %s, want %s", got, want)
	}
	if routes, known := rt.RoutesFor("AccountBlocked"); !known || len(routes) != 0 {
		t.Errorf("AccountBlocked: routes=%v known=%v, want known and with no consumer yet", routes, known)
	}
}

// RN-WRK7-09: a revocation of the sessions of an account reaches identity-audit, which keeps the cutoff.
func TestC_default_routes_send_session_revocations_to_identity_audit(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	routes, known := rt.RoutesFor("AccountSessionsRevoked")
	want := out.Route{Consumer: "identity-audit", Path: "/api/v1/identity-audit/internal/tenants/{tenantId}/events"}
	if !known || len(routes) != 1 || routes[0] != want {
		t.Errorf("AccountSessionsRevoked: routes=%v known=%v, want %v", routes, known, want)
	}
}
