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

// RN-WRK7-16: the second path of a route is checked at start, so a typo stops the worker and does not fail every event of a platform account.
func TestC_routing_table_refuses_an_odd_path_without_tenant(t *testing.T) {
	tenantPath := "/internal/tenants/{tenantId}/events"
	odd := map[string]string{
		"relative":                 "internal/events",
		"holds the placeholder":    "/internal/{tenantId}/events",
		"holds a space":            "/internal/events ",
		"a full address":           "https://audit/internal/events",
		"a network-path reference": "//audit/internal/events",
		"a dot segment":            "/internal/../events",
		"a query":                  "/internal/events?x=1",
		"a fragment":               "/internal/events#x",
		"a percent escape":         "/internal/%2e%2e/events",
		"a backslash":              `/internal\events`,
		"a closing brace":          "/internal/events}",
		"a tab":                    "/internal/events\t",
	}
	for name, noTenant := range odd {
		routes := map[string][]out.Route{"AccountBlocked": {{Consumer: "audit", Path: tenantPath, NoTenantPath: noTenant}}}
		if _, err := config.NewRoutingTable(routes); err == nil {
			t.Errorf("%s (%q): the table was accepted, want an error at start", name, noTenant)
		}
	}
	good := map[string][]out.Route{"AccountBlocked": {{Consumer: "audit", Path: tenantPath, NoTenantPath: "/internal/events"}}}
	if _, err := config.NewRoutingTable(good); err != nil {
		t.Errorf("a plain absolute path without tenant was refused: %v", err)
	}
	noPlaceholder := map[string][]out.Route{"AccountBlocked": {{Consumer: "audit", Path: "/internal/events", NoTenantPath: "/internal/platform-events"}}}
	if _, err := config.NewRoutingTable(noPlaceholder); err == nil {
		t.Error("a path without tenant on a route whose path has no tenant was accepted: it would be ignored in silence")
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
}

// RN-WRK7-09 and RN-WRK7-11: the six events of the security microservice that the audit records reach identity-audit,
// by the path of the tenant and, for an account that belongs to none, by the path without tenant.
func TestC_default_routes_send_the_audited_security_events_to_identity_audit(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	want := out.Route{
		Consumer:     "identity-audit",
		Path:         "/api/v1/identity-audit/internal/tenants/{tenantId}/events",
		NoTenantPath: "/api/v1/identity-audit/internal/events",
	}
	for _, eventType := range []string{"AccountSessionsRevoked", "AccountBlocked", "SpecialPermissionGranted",
		"SpecialPermissionRevoked", "OptionalRoleEnabled", "OptionalRoleDisabled"} {
		routes, known := rt.RoutesFor(eventType)
		if !known || len(routes) != 1 || routes[0] != want {
			t.Errorf("%s: routes=%v known=%v, want %v", eventType, routes, known, want)
		}
	}
}

// RN-WRK7-15: nothing consumes these two yet; they are known, so they are published without delivery and raise no alert.
func TestC_credential_and_email_events_are_known_and_have_no_consumer(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	for _, eventType := range []string{"AccountTemporaryCredentialIssued", "AccountEmailVerified"} {
		if routes, known := rt.RoutesFor(eventType); !known || len(routes) != 0 {
			t.Errorf("%s: routes=%v known=%v, want known and with no consumer", eventType, routes, known)
		}
	}
}
