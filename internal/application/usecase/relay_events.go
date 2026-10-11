package usecase

import (
	"context"
	"log/slog"

	workerruntime "github.com/code-corhuila/multi-tour-worker-runtime"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/port/out"
)

// Publisher is a named outbox to read from.
type Publisher struct {
	Name   string
	Source out.EventSource
}

// Router tells where an event type goes.
type Router interface {
	RoutesFor(eventType string) ([]out.Route, bool)
}

// RelayEvents is job 7.
type RelayEvents struct{}

var _ workerruntime.Job = (*RelayEvents)(nil)

// NewRelayEvents wires the job.
func NewRelayEvents(publishers []Publisher, router Router, sink out.EventSink, log *slog.Logger) *RelayEvents {
	return &RelayEvents{}
}

// Name identifies the job in the runtime.
func (r *RelayEvents) Name() string { return "event-relay" }

// Run relays at most batch events.
func (r *RelayEvents) Run(ctx context.Context, batch int) (processed, failed int) {
	return 0, 0
}
