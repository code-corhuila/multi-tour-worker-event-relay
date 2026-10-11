package config

import (
	"fmt"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/port/out"
)

const tenantEventsPath = "/api/v1/security/internal/tenants/{tenantId}/events"

// knownEventTypes are the types the publishers write to their outbox in this cut. AccountBlocked is\n// written by the outbox of multi-tour-security; it is a proposed name that is not in 06-data yet.
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
		}
	}
	return &RoutingTable{routes: routes}, nil
}

// DefaultRoutes is the table of this cut. Only security exposes a receiving route today.
// AccountSessionsRevoked and AccountBlocked have no consumer yet and are marked published without
// delivery on purpose, so they do not pile up in the outbox.
// shortcut: spec AC-01.2 (these events reach every service) is not met until a consumer exists,
// upgrade when identity-audit or another service publishes its receiving route.
func DefaultRoutes() map[string][]out.Route {
	security := []out.Route{{Consumer: "security", Path: tenantEventsPath}}
	return map[string][]out.Route{
		"TenantCreated":          security,
		"TenantInactivated":      security,
		"TenantReactivated":      security,
		"AccountSessionsRevoked": nil,
		"AccountBlocked":         nil,
	}
}

// RoutesFor returns the routes of an event type and whether the type is in the table.
// A known type with no routes is delivered to nobody and counts as published.
func (t *RoutingTable) RoutesFor(eventType string) ([]out.Route, bool) {
	routes, known := t.routes[eventType]
	return routes, known
}
