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

// Relay spec 007 (AC-RL-07.1): the tenant events reach security and the copy of the status of the tenant that commercial-reservation keeps.
func TestDefaultRoutesAreValidAndSendTenantEventsToSecurityAndReservations(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	for _, eventType := range []string{"TenantCreated", "TenantInactivated", "TenantReactivated"} {
		routes, known := rt.RoutesFor(eventType)
		want := []out.Route{
			{Consumer: "security", Path: "/api/v1/security/internal/tenants/{tenantId}/events"},
			{Consumer: "commercial-reservation", Path: "/api/v1/commercial-reservation/internal/tenants/{tenantId}/events"},
		}
		if !known || fmt.Sprint(routes) != fmt.Sprint(want) {
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
	if got, want := fmt.Sprint(rt.Consumers()), "[commercial-reservation identity-audit notifications security]"; got != want {
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

// RN-WRK7-15: nothing consumes the temporary credential yet; it is known, so it is published without delivery and raises no alert.
func TestC_the_credential_event_is_known_and_has_no_consumer(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	if routes, known := rt.RoutesFor("AccountTemporaryCredentialIssued"); !known || len(routes) != 0 {
		t.Errorf("AccountTemporaryCredentialIssued: routes=%v known=%v, want known and with no consumer", routes, known)
	}
}

// Relay spec 007 (AC-RL-07.2): a verified account is linked to the customer of its tenant by commercial-reservation and audited.
func TestC_a_verified_account_reaches_commercial_reservation_and_the_audit(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	routes, known := rt.RoutesFor("AccountEmailVerified")
	if !known || len(routes) != 2 || routes[0].Consumer != "commercial-reservation" || routes[1].Consumer != "identity-audit" {
		t.Fatalf("AccountEmailVerified: routes=%v known=%v, want commercial-reservation then identity-audit", routes, known)
	}
	if routes[0].Path != "/api/v1/commercial-reservation/internal/tenants/{tenantId}/events" ||
		routes[1].Path != "/api/v1/identity-audit/internal/tenants/{tenantId}/events" {
		t.Errorf("paths = %q, %q", routes[0].Path, routes[1].Path)
	}
}

// Relay spec 007 (AC-RL-07.4): the events that commercial-reservation writes are audited (`domain-events.md`, consumer: Audit).
func TestC_every_audited_event_of_commercial_reservation_goes_to_identity_audit(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	types := []string{"ReservationCreated", "ReservationModified", "CapacityAllocated", "CapacityReleased", "PaymentEvidenceRegistered",
		"PaymentValidated", "PaymentRejected", "ReservationConfirmed", "ReservationCancelled", "RefundAuthorized",
		"CustomerRegistered", "DiscountRuleChanged", "SensitiveDataAccessed"}
	for _, eventType := range types {
		routes, known := rt.RoutesFor(eventType)
		if !known || len(routes) != 1 || routes[0].Consumer != "identity-audit" ||
			routes[0].Path != "/api/v1/identity-audit/internal/tenants/{tenantId}/events" {
			t.Errorf("%s: routes=%v known=%v, want identity-audit on the route of its tenant", eventType, routes, known)
		}
	}
}

// HU-OPS-001 AC5: a scheduled departure reaches commercial-reservation, which creates the capacity row from it, by the one route
// where that service receives events. Its payload is what operations-cost writes.
func TestC_default_routes_send_the_scheduled_departure_to_commercial_reservation(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	want := out.Route{Consumer: "commercial-reservation", Path: "/api/v1/commercial-reservation/internal/tenants/{tenantId}/events"}
	routes, known := rt.RoutesFor("SalidaScheduled")
	if !known || len(routes) != 1 || routes[0] != want {
		t.Errorf("SalidaScheduled: routes=%v known=%v, want %v", routes, known, want)
	}
}

// Gap G-API-2: commercial-reservation does not accept SalidaRescheduled (its route answers 400 to a type it does not name, and the
// event would end FAILED after twenty attempts). It is known, so it is published without delivery and raises no alert, until
// that service accepts it; then it gets the same route as SalidaScheduled.
func TestC_the_rescheduled_departure_is_known_and_has_no_consumer_until_commercial_reservation_accepts_it(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	if routes, known := rt.RoutesFor("SalidaRescheduled"); !known || len(routes) != 0 {
		t.Errorf("SalidaRescheduled: routes=%v known=%v, want known and with no consumer", routes, known)
	}
}

// HU-OPS-003 part B, relay spec 006: operations-cost writes the mark of a departure past its estimated end, and no service accepts it
// yet (commercial-reservation rejects the types it does not name), so it is known and published without delivery.
func TestC_the_mark_of_a_departure_is_known_and_has_no_consumer_until_a_service_accepts_it(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	if routes, known := rt.RoutesFor("SalidaMarkedPendienteDeRegularizacion"); !known || len(routes) != 0 {
		t.Errorf("SalidaMarkedPendienteDeRegularizacion: routes=%v known=%v, want known and with no consumer", routes, known)
	}
}

// HU-SEC-004, relay spec 003: the facts of a password recovery reach the audit, also for the Platform Administrator.
var passwordRecoveryFacts = []string{"PasswordRecoveryRequested", "PasswordReset", "PasswordResetFailed"}

func TestNewRoutingTable_KnowsThePasswordRecoveryFacts(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	for _, eventType := range passwordRecoveryFacts {
		if _, known := rt.RoutesFor(eventType); !known {
			t.Errorf("%s is not known to the routing table", eventType)
		}
	}
}

func TestDefaultRoutes_PasswordRecoveryFactsGoToTheAudit(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	for _, eventType := range passwordRecoveryFacts {
		routes, _ := rt.RoutesFor(eventType)
		if len(routes) == 0 || routes[0].Consumer != "identity-audit" ||
			routes[0].Path != "/api/v1/identity-audit/internal/tenants/{tenantId}/events" {
			t.Errorf("%s: routes=%v, want the first route to identity-audit by tenant", eventType, routes)
		}
	}
}

func TestDefaultRoutes_PasswordRecoveryFactsOfThePlatformUseTheNoTenantPath(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	for _, eventType := range passwordRecoveryFacts {
		routes, _ := rt.RoutesFor(eventType)
		if len(routes) == 0 || routes[0].NoTenantPath != "/api/v1/identity-audit/internal/events" {
			t.Errorf("%s: routes=%v, want the path without tenant of identity-audit", eventType, routes)
		}
	}
}

// HU-WRK-004, relay spec 009: the two facts of a reset that job 4 tells the owner about also reach notifications, by tenant and, for the
// Platform Administrator, with no tenant; the request for a code is not one of them.
func TestRoutes_the_reset_facts_reach_the_audit_and_notifications(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	for _, eventType := range []string{"PasswordReset", "PasswordResetFailed"} {
		routes, _ := rt.RoutesFor(eventType)
		if len(routes) != 2 || routes[1].Consumer != "notifications" ||
			routes[1].Path != "/api/v1/notifications/internal/tenants/{tenantId}/events" ||
			routes[1].NoTenantPath != "/api/v1/notifications/internal/events" {
			t.Errorf("%s: routes=%v, want the audit and then notifications", eventType, routes)
		}
	}
	if routes, _ := rt.RoutesFor("PasswordRecoveryRequested"); len(routes) != 1 {
		t.Errorf("PasswordRecoveryRequested: routes=%v, want only the audit", routes)
	}
}

// HU-SEC-007, relay spec 010: the two facts of the exceptional access of the Platform Administrator to a tenant reach the audit, by the
// route of that tenant (they always have one) and nobody else.
// Spec 012 (G-SEC9-1) adds the third: each use of the token of that access, which operations-cost and the services of commercial-reservation and cash-reporting write.
var supportAccessFacts = []string{"PlatformExceptionalAccessOpened", "PlatformExceptionalAccessClosed", "PlatformExceptionalAccessUsed"}

func TestNewRoutingTable_KnowsTheSupportAccessFacts(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	for _, eventType := range supportAccessFacts {
		if _, known := rt.RoutesFor(eventType); !known {
			t.Errorf("%s is not known to the routing table", eventType)
		}
	}
}

func TestDefaultRoutes_SupportAccessFactsGoToTheAudit(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	for _, eventType := range supportAccessFacts {
		routes, _ := rt.RoutesFor(eventType)
		if len(routes) != 1 || routes[0].Consumer != "identity-audit" ||
			routes[0].Path != "/api/v1/identity-audit/internal/tenants/{tenantId}/events" {
			t.Errorf("%s: routes=%v, want only the route to identity-audit by tenant", eventType, routes)
		}
	}
}

// HU-OPS-002, relay spec 004: the new total of the vehicles of a departure reaches commercial-reservation, which already
// updates the seats of the departure from it, by the same route as the scheduled departure.
func TestNewRoutingTable_KnowsTheVehicleChange(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	if _, known := rt.RoutesFor("SalidaVehiclesChanged"); !known {
		t.Error("SalidaVehiclesChanged is not a known event type")
	}
}

func TestC_default_routes_send_the_vehicle_change_to_commercial_reservation(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	want := out.Route{Consumer: "commercial-reservation", Path: "/api/v1/commercial-reservation/internal/tenants/{tenantId}/events"}
	routes, known := rt.RoutesFor("SalidaVehiclesChanged")
	if !known || len(routes) != 1 || routes[0] != want {
		t.Errorf("SalidaVehiclesChanged: routes=%v known=%v, want %v", routes, known, want)
	}
}

// HU-OPS-003, relay spec 005: the start and the end of a departure reach commercial-reservation, which already accepts both on
// the route of the scheduled departure.
func TestNewRoutingTable_KnowsTheStartAndTheEnd(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	for _, eventType := range []string{"SalidaExecutionStarted", "SalidaFinalized"} {
		if _, known := rt.RoutesFor(eventType); !known {
			t.Errorf("%s is not a known event type", eventType)
		}
	}
}

func TestC_default_routes_send_the_start_and_the_end_to_commercial_reservation(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	want := out.Route{Consumer: "commercial-reservation", Path: "/api/v1/commercial-reservation/internal/tenants/{tenantId}/events"}
	for _, eventType := range []string{"SalidaExecutionStarted", "SalidaFinalized"} {
		routes, known := rt.RoutesFor(eventType)
		if !known || len(routes) != 1 || routes[0] != want {
			t.Errorf("%s: routes=%v known=%v, want %v", eventType, routes, known, want)
		}
	}
}

// HU-SEC-007 part C, relay spec 011: the notice of an emergency access reaches notifications by the route of its tenant and does not reach the audit,
// whose record of the access are the facts it comes with.
func TestRoutes_the_emergency_notice_reaches_notifications(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	routes, known := rt.RoutesFor("EmergencyAccessNoticeRequested")
	if !known || len(routes) != 1 || routes[0].Consumer != "notifications" || routes[0].Path != "/api/v1/notifications/internal/tenants/{tenantId}/events" {
		t.Fatalf("routes=%v known=%v, want notifications by the route of the tenant", routes, known)
	}
}

func TestRoutes_the_emergency_notice_does_not_reach_the_audit(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	routes, _ := rt.RoutesFor("EmergencyAccessNoticeRequested")
	for _, route := range routes {
		if route.Consumer == "identity-audit" {
			t.Errorf("the notice reaches the audit: %v", routes)
		}
	}
	// the facts of the access keep reaching the audit, as before
	for _, eventType := range supportAccessFacts {
		if routes, _ := rt.RoutesFor(eventType); len(routes) != 1 || routes[0].Consumer != "identity-audit" {
			t.Errorf("%s: routes=%v, want only the audit", eventType, routes)
		}
	}
}

// HU-COM-015 (DP-15-01, G-E): the notice of a customer is one ClientNotified row. It reaches job 4, which sends the e-mail, and keeps reaching the audit as evidence.
func TestRoutes_the_client_notice_reaches_notifications_and_the_audit(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	routes, known := rt.RoutesFor("ClientNotified")
	if !known || len(routes) != 2 {
		t.Fatalf("routes=%v known=%v, want two routes", routes, known)
	}
	if routes[0].Consumer != "identity-audit" || routes[0].Path != "/api/v1/identity-audit/internal/tenants/{tenantId}/events" {
		t.Errorf("first route=%v, want the audit by the route of its tenant", routes[0])
	}
	if routes[1].Consumer != "notifications" || routes[1].Path != "/api/v1/notifications/internal/tenants/{tenantId}/events" {
		t.Errorf("second route=%v, want notifications by the route of its tenant", routes[1])
	}
}

// G-RL7-1: the relay reads the outbox of cash-reporting, which writes nine events no service consumes yet (spec 007 leaves them out of scope). They are
// known and have no consumer, so each one is published without delivery and does not end failed after twenty attempts.
func TestC_the_events_of_cash_reporting_are_known_and_have_no_consumer(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	for _, name := range []string{"CashSessionOpened", "CashSessionClosed", "CashSessionAutoCreated", "CashSessionAutoClosed", "CashSessionReopened",
		"CashIncomeRecorded", "OperationalPaymentRecorded", "OperationalExpenseRecorded", "RefundExecuted"} {
		if routes, known := rt.RoutesFor(name); !known || len(routes) != 0 {
			t.Errorf("%s: routes=%v known=%v, want known and with no consumer", name, routes, known)
		}
	}
}

// HU-IDA-004 (spec 010, D-06): the change of the zone or the country of a tenant is known to the table and has no consumer yet. security and
// commercial-reservation answer 400 to a tenant event type they do not know, which would end it FAILED after twenty attempts, and the audit
// record is written by the identity-audit API with the change itself. So it is published without delivery, as SalidaRescheduled is, until a
// consumer accepts it.
func TestDefaultRoutes_TenantLocalizationChangedIsKnownAndHasNoConsumerYet(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	routes, known := rt.RoutesFor("TenantLocalizationChanged")
	if !known || len(routes) != 0 {
		t.Errorf("TenantLocalizationChanged: routes=%v known=%v, want known with no route", routes, known)
	}
}
