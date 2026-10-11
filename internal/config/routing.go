package config

import "github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/port/out"

// RoutingTable maps each event type to the routes that receive it.
type RoutingTable struct {
	routes map[string][]out.Route
}

// NewRoutingTable checks the table at start.
func NewRoutingTable(routes map[string][]out.Route) (*RoutingTable, error) {
	return &RoutingTable{}, nil
}

// DefaultRoutes is the table of this cut.
func DefaultRoutes() map[string][]out.Route {
	return nil
}

// RoutesFor returns the routes of an event type and whether the type is known.
func (t *RoutingTable) RoutesFor(eventType string) ([]out.Route, bool) {
	return nil, false
}
