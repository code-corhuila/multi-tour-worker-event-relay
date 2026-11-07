package config

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/port/out"
)

const (
	tenantEventsPath  = "/api/v1/security/internal/tenants/{tenantId}/events"
	sessionEventsPath = "/api/v1/identity-audit/internal/tenants/{tenantId}/events"
)

// knownEventTypes are the types the publishers write to their outbox in this cut. AccountBlocked is
// written by the outbox of multi-tour-security; it is a proposed name that is not in 06-data yet.
var knownEventTypes = map[string]bool{
	"TenantCreated":          true,
	"TenantInactivated":      true,
	"TenantReactivated":      true,
	"AccountSessionsRevoked": true,
	"AccountBlocked":         true,
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

// DefaultRoutes is the table of this cut. Security receives the tenant events and identity-audit the session
// revocations. AccountBlocked has no consumer yet and is marked published without delivery on purpose, so it does
// not pile up in the outbox.
// shortcut: spec AC-01.2 (these events reach every service) is met only for the services that expose a receiving
// route; add one line per consumer when commercial-reservation, operations-cost or cash-reporting publish theirs.
func DefaultRoutes() map[string][]out.Route {
	security := []out.Route{{Consumer: "security", Path: tenantEventsPath}}
	identityAudit := []out.Route{{Consumer: "identity-audit", Path: sessionEventsPath}}
	return map[string][]out.Route{
		"TenantCreated":          security,
		"TenantInactivated":      security,
		"TenantReactivated":      security,
		"AccountSessionsRevoked": identityAudit,
		"AccountBlocked":         nil,
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
