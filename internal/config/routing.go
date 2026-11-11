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

	// job 4 receives the facts of a reset it tells the owner about (relay spec 009) and the notices of the customers (HU-COM-015)
	notificationsEventsPath         = "/api/v1/notifications/internal/tenants/{tenantId}/events"
	notificationsEventsNoTenantPath = "/api/v1/notifications/internal/events"
)

// knownEventTypes are the types the publishers write to their outbox in this cut. The names of the events of
// multi-tour-security other than AccountSessionsRevoked are proposals that are not in 02-domain yet (gap G-02-EVT).
var knownEventTypes = map[string]bool{
	"TenantCreated":                         true,
	"TenantInactivated":                     true,
	"TenantReactivated":                     true,
	"AccountSessionsRevoked":                true,
	"AccountBlocked":                        true,
	"SpecialPermissionGranted":              true,
	"SpecialPermissionRevoked":              true,
	"OptionalRoleEnabled":                   true,
	"OptionalRoleDisabled":                  true,
	"AccountTemporaryCredentialIssued":      true,
	"AccountEmailVerified":                  true,
	"PasswordRecoveryRequested":             true,
	"PasswordReset":                         true,
	"PasswordResetFailed":                   true,
	"PlatformExceptionalAccessOpened":       true,
	"PlatformExceptionalAccessClosed":       true,
	"EmergencyAccessNoticeRequested":        true,
	"SalidaScheduled":                       true,
	"SalidaRescheduled":                     true,
	"SalidaVehiclesChanged":                 true,
	"SalidaExecutionStarted":                true,
	"SalidaFinalized":                       true,
	"SalidaMarkedPendienteDeRegularizacion": true,
	// The events that commercial-reservation writes (relay spec 007; the consumer of each one is the audit, domain-events.md).
	"ReservationCreated":        true,
	"ReservationModified":       true,
	"CapacityAllocated":         true,
	"CapacityReleased":          true,
	"PaymentEvidenceRegistered": true,
	"PaymentValidated":          true,
	"PaymentRejected":           true,
	"ReservationConfirmed":      true,
	"ReservationCancelled":      true,
	"RefundAuthorized":          true,
	"ClientNotified":            true,
	"CustomerRegistered":        true,
	"DiscountRuleChanged":       true,
	"SensitiveDataAccessed":     true,
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

// DefaultRoutes is the table of this cut. Security receives the tenant events; identity-audit the eleven events of the
// security microservice that the audit records (HU-IDA-002, the three of the password recovery of HU-SEC-004 and the two of the
// exceptional access of HU-SEC-007); notifications alone the notice of an emergency access (EmergencyAccessNoticeRequested,
// relay spec 011), which the audit does not record because it has the facts of the access;
// commercial-reservation the scheduled departure of operations-cost, the new total of its vehicles (HU-OPS-002) and its
// start and end (HU-OPS-003). The tenant events also reach commercial-reservation (the copy of the status of the tenant), and
// AccountEmailVerified reaches it (it links the account to the customer of that tenant) and the audit; the 14 events that
// commercial-reservation writes reach the audit (relay spec 007). AccountTemporaryCredentialIssued, SalidaRescheduled and
// SalidaMarkedPendienteDeRegularizacion have no consumer yet and are marked published without delivery on purpose, so they do not
// pile up in the outbox.
// shortcut: spec AC-01.2 (the tenant events and AccountSessionsRevoked reach every service) is met only in part: cash-reporting and
// operations-cost have no receiving route and nothing reaches commercial-reservation about revoked sessions; add one line per
// consumer as each service exposes its route. The relay reads the outbox of cash-reporting (G-RL7-1); its PlatformExceptionalAccessUsed reaches the audit by event type.
func DefaultRoutes() map[string][]out.Route {
	security := []out.Route{{Consumer: "security", Path: tenantEventsPath}}
	identityAudit := []out.Route{{Consumer: "identity-audit", Path: auditEventsPath, NoTenantPath: auditEventsNoTenantPath}}
	reservations := []out.Route{{Consumer: "commercial-reservation", Path: reservationEventsPath}}
	securityAndReservations := []out.Route{security[0], reservations[0]}
	notifications := out.Route{Consumer: "notifications", Path: notificationsEventsPath, NoTenantPath: notificationsEventsNoTenantPath}
	auditAndNotifications := []out.Route{identityAudit[0], notifications}
	return map[string][]out.Route{
		"TenantCreated":             securityAndReservations,
		"TenantInactivated":         securityAndReservations,
		"TenantReactivated":         securityAndReservations,
		"AccountSessionsRevoked":    identityAudit,
		"AccountBlocked":            identityAudit,
		"SpecialPermissionGranted":  identityAudit,
		"SpecialPermissionRevoked":  identityAudit,
		"OptionalRoleEnabled":       identityAudit,
		"OptionalRoleDisabled":      identityAudit,
		"PasswordRecoveryRequested": identityAudit,
		"PasswordReset":             auditAndNotifications,
		"PasswordResetFailed":       auditAndNotifications,
		// the exceptional access of the Platform Administrator to a tenant: always a tenant, only the audit (relay spec 010)
		"PlatformExceptionalAccessOpened": identityAudit,
		"PlatformExceptionalAccessClosed": identityAudit,
		// the notice that an emergency access opened or ended, one per active Tenant Administrator: only job 4, by the route of its tenant; the audit
		// has the facts of the access (relay spec 011)
		"EmergencyAccessNoticeRequested":   {notifications},
		"AccountTemporaryCredentialIssued": nil,
		// commercial-reservation links the account to the customer of that tenant and the audit records it (relay spec 007).
		"AccountEmailVerified":   {reservations[0], identityAudit[0]},
		"SalidaScheduled":        reservations,
		"SalidaVehiclesChanged":  reservations,
		"SalidaExecutionStarted": reservations,
		"SalidaFinalized":        reservations,
		// Gap G-API-2: commercial-reservation does not accept SalidaRescheduled yet (it would answer 400 and the event would end
		// FAILED after twenty attempts), so it is published without delivery until it does; then it takes the same route.
		"SalidaRescheduled": nil,
		// HU-OPS-003 part B: operations-cost marks a departure past its estimated end; no service accepts the event yet, so it is
		// published without delivery until one does.
		"SalidaMarkedPendienteDeRegularizacion": nil,
		// The events of commercial-reservation reach the audit (relay spec 007, AC-RL-07.4).
		"ReservationCreated":        identityAudit,
		"ReservationModified":       identityAudit,
		"CapacityAllocated":         identityAudit,
		"CapacityReleased":          identityAudit,
		"PaymentEvidenceRegistered": identityAudit,
		"PaymentValidated":          identityAudit,
		"PaymentRejected":           identityAudit,
		"ReservationConfirmed":      identityAudit,
		"ReservationCancelled":      identityAudit,
		"RefundAuthorized":          identityAudit,
		// HU-COM-015 (DP-15-01): the notice of a customer is sent by job 4 and kept by the audit as evidence; its payload holds no personal datum.
		"ClientNotified":        auditAndNotifications,
		"CustomerRegistered":    identityAudit,
		"DiscountRuleChanged":   identityAudit,
		"SensitiveDataAccessed": identityAudit,
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
