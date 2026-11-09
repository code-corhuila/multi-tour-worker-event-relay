package config

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/port/out"
)

const (
	tenantEventsPath = "/api/v1/security/internal/tenants/{tenantId}/events"
	// identity-audit records the events of the security microservice, by the path of a tenant and, for an account that
	// belongs to none, by the path without tenant (ADR-004 D4).
	auditEventsPath         = "/api/v1/identity-audit/internal/tenants/{tenantId}/events"
	auditEventsNoTenantPath = "/api/v1/identity-audit/internal/events"
	// commercial-reservation keeps the capacity row of each departure and creates it from SalidaScheduled (ADR-004 D4, HU-OPS-001 AC5).
	reservationEventsPath = "/api/v1/commercial-reservation/internal/tenants/{tenantId}/events"
)

// knownEventTypes are the types the publishers write to their outbox in this cut. The names of the events of
// multi-tour-security other than AccountSessionsRevoked are proposals that are not in 02-domain yet (gap G-02-EVT).
var knownEventTypes = map[string]bool{
	"TenantCreated":                    true,
	"TenantInactivated":                true,
	"TenantReactivated":                true,
	"AccountSessionsRevoked":           true,
	"AccountBlocked":                   true,
	"SpecialPermissionGranted":         true,
	"SpecialPermissionRevoked":         true,
	"OptionalRoleEnabled":              true,
	"OptionalRoleDisabled":             true,
	"AccountTemporaryCredentialIssued": true,
	"AccountEmailVerified":             true,
	"PasswordRecoveryRequested":        true,
	"PasswordReset":                    true,
	"PasswordResetFailed":              true,
	"SalidaScheduled":                  true,
	"SalidaRescheduled":                true,
	"SalidaVehiclesChanged":            true,
	"SalidaExecutionStarted":           true,
	"SalidaFinalized":                  true,
}

// RoutingTable maps each event type to the routes that receive it.
type RoutingTable struct {
	routes map[string][]out.Route
}

// NewRoutingTable checks the table at start: an unknown event type or an incomplete route stops the worker.
func NewRoutingTable(routes map[string][]out.Route) (*RoutingTable, error) {
	for eventType, list := range routes {
		if !knownEventTypes[eventType] {
			return nil, fmt.Errorf("routing table: unknown event type %q", eventType)
		}
		for _, route := range list {
			if route.Consumer == "" || route.Path == "" {
				return nil, fmt.Errorf("routing table: event type %q has a route without consumer or path", eventType)
			}
			if route.NoTenantPath != "" && !plainAbsolutePath(route.NoTenantPath) {
				return nil, fmt.Errorf("routing table: event type %q has a path without tenant that is not a plain absolute path", eventType)
			}
			if route.NoTenantPath != "" && !strings.Contains(route.Path, "{tenantId}") {
				// the path without tenant is chosen only where the placeholder is, so here it would be ignored in silence
				return nil, fmt.Errorf("routing table: event type %q has a path without tenant on a route whose path has no tenant", eventType)
			}
		}
	}
	return &RoutingTable{routes: routes}, nil
}

// plainAbsolutePath is a path on the consumer's own host and nothing else: it starts with one slash and holds no
// placeholder, query, fragment, percent escape, backslash, dot segment, space or control character, so it cannot name
// another host, climb out of its prefix or leave a tenant out by a typo.
func plainAbsolutePath(path string) bool {
	if !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || strings.ContainsAny(path, "{}?#%\\") {
		return false
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == ".." || segment == "." {
			return false
		}
	}
	return !strings.ContainsFunc(path, func(c rune) bool { return unicode.IsSpace(c) || unicode.IsControl(c) })
}

// DefaultRoutes is the table of this cut. Security receives the tenant events; identity-audit the nine events of the
// security microservice that the audit records (HU-IDA-002, and the three of the password recovery of HU-SEC-004);
// commercial-reservation the scheduled departure of operations-cost and the new total of its vehicles (HU-OPS-002), and the start and the end of a departure (HU-OPS-003). AccountTemporaryCredentialIssued and
// SalidaRescheduled have no consumer yet; AccountEmailVerified is accepted by commercial-reservation (HU-COM-005) but is
// not routed to it yet. The three are marked published without delivery on purpose, so they do not pile up in the outbox.
// shortcut: spec AC-01.2 (TenantInactivated, TenantReactivated and AccountSessionsRevoked reach every service) is met
// only in part; add one line per consumer as each service exposes its receiving route (commercial-reservation already
// accepts the tenant events).
func DefaultRoutes() map[string][]out.Route {
	security := []out.Route{{Consumer: "security", Path: tenantEventsPath}}
	identityAudit := []out.Route{{Consumer: "identity-audit", Path: auditEventsPath, NoTenantPath: auditEventsNoTenantPath}}
	reservations := []out.Route{{Consumer: "commercial-reservation", Path: reservationEventsPath}}
	return map[string][]out.Route{
		"TenantCreated":                    security,
		"TenantInactivated":                security,
		"TenantReactivated":                security,
		"AccountSessionsRevoked":           identityAudit,
		"AccountBlocked":                   identityAudit,
		"SpecialPermissionGranted":         identityAudit,
		"SpecialPermissionRevoked":         identityAudit,
		"OptionalRoleEnabled":              identityAudit,
		"OptionalRoleDisabled":             identityAudit,
		"PasswordRecoveryRequested":        identityAudit,
		"PasswordReset":                    identityAudit,
		"PasswordResetFailed":              identityAudit,
		"AccountTemporaryCredentialIssued": nil,
		"AccountEmailVerified":             nil,
		"SalidaScheduled":                  reservations,
		"SalidaVehiclesChanged":            reservations,
		"SalidaExecutionStarted":           reservations,
		"SalidaFinalized":                  reservations,
		// Gap G-API-2: commercial-reservation does not accept SalidaRescheduled yet (it would answer 400 and the event would end
		// FAILED after twenty attempts), so it is published without delivery until it does; then it takes the same route.
		"SalidaRescheduled": nil,
	}
}

// Consumers returns the sorted names of every consumer the table delivers to.
func (t *RoutingTable) Consumers() []string {
	seen := map[string]bool{}
	for _, list := range t.routes {
		for _, route := range list {
			seen[route.Consumer] = true
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// RoutesFor returns the routes of an event type and whether the type is in the table.
// A known type with no routes is delivered to nobody and counts as published.
func (t *RoutingTable) RoutesFor(eventType string) ([]out.Route, bool) {
	routes, known := t.routes[eventType]
	return routes, known
}
