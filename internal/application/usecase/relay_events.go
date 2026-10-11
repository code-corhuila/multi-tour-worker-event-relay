package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"unicode"

	workerruntime "github.com/code-corhuila/multi-tour-worker-runtime"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/port/out"
)

const (
	pageLimit         = 100
	maxErrorRunes     = 500
	tenantPlaceholder = "{tenantId}"
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

// RelayEvents is job 7: read, route, deliver, then mark published or record the failure.
type RelayEvents struct {
	publishers []Publisher
	router     Router
	sink       out.EventSink
	log        *slog.Logger
}

var _ workerruntime.Job = (*RelayEvents)(nil)

// NewRelayEvents wires the job.
func NewRelayEvents(publishers []Publisher, router Router, sink out.EventSink, log *slog.Logger) *RelayEvents {
	return &RelayEvents{publishers: publishers, router: router, sink: sink, log: log}
}

// Name identifies the job in the runtime.
func (r *RelayEvents) Name() string { return "event-relay" }

// Run relays at most batch events across all publishers.
func (r *RelayEvents) Run(ctx context.Context, batch int) (processed, failed int) {
	for _, publisher := range r.publishers {
		p, f := r.drain(ctx, publisher, batch-processed-failed)
		processed, failed = processed+p, failed+f
	}
	return processed, failed
}

// drain reads page 1 until it is empty or the budget is spent: a handled event leaves the list,
// so page 1 is always the next one (the total and page count are not snapshot-consistent).
// An event that stays pending after being handled is seen once per run, never looped on.
func (r *RelayEvents) drain(ctx context.Context, publisher Publisher, budget int) (processed, failed int) {
	seen := map[string]bool{}
	log := r.log.With("publisher", publisher.Name)
	for budget > 0 && ctx.Err() == nil {
		events, err := publisher.Source.ListPending(ctx, 1, min(budget, pageLimit))
		if err != nil {
			log.WarnContext(ctx, "reading the outbox failed, left for the next run", "error", sanitize(err))
			return processed, failed + 1
		}
		fresh := events[:0:0]
		for _, e := range events {
			if !seen[e.ID] {
				seen[e.ID] = true
				fresh = append(fresh, e)
			}
		}
		if len(fresh) == 0 {
			return processed, failed
		}
		p, f := workerruntime.Each(ctx, log, fresh, func(ctx context.Context, e out.Event) error {
			return r.relay(ctx, publisher.Source, e)
		})
		processed, failed = processed+p, failed+f
		budget -= len(fresh)
	}
	return processed, failed
}

// relay delivers one event to all its routes; a failure is recorded in the source and returned.
func (r *RelayEvents) relay(ctx context.Context, source out.EventSource, e out.Event) error {
	routes, known := r.router.RoutesFor(e.Type)
	if !known {
		return r.fail(ctx, source, e, fmt.Errorf("unknown event type %q", e.Type))
	}
	var errs []error
	for _, route := range routes {
		if err := r.deliver(ctx, route, e); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", route.Consumer, err))
		}
	}
	if len(errs) > 0 {
		return r.fail(ctx, source, e, errors.Join(errs...))
	}
	return source.MarkPublished(ctx, e.ID)
}

func (r *RelayEvents) deliver(ctx context.Context, route out.Route, e out.Event) error {
	path := route.Path
	if strings.Contains(path, tenantPlaceholder) {
		if e.TenantID == "" {
			return errors.New("event has no tenantId for the route")
		}
		path = strings.ReplaceAll(path, tenantPlaceholder, url.PathEscape(e.TenantID))
	}
	return r.sink.Deliver(ctx, out.Delivery{Consumer: route.Consumer, Path: path, IdempotencyKey: e.ID, Event: e})
}

// fail records the attempt and returns the cause so the runtime counts the event as failed.
func (r *RelayEvents) fail(ctx context.Context, source out.EventSource, e out.Event, cause error) error {
	if err := source.RecordFailedAttempt(ctx, e.ID, sanitize(cause)); err != nil {
		return errors.Join(cause, fmt.Errorf("recording the attempt: %w", err))
	}
	return cause
}

// sanitize makes the error one printable line of bounded length, as the attempts route requires.
func sanitize(err error) string {
	line := strings.Join(strings.FieldsFunc(err.Error(), func(c rune) bool { return unicode.IsControl(c) || unicode.IsSpace(c) }), " ")
	if runes := []rune(line); len(runes) > maxErrorRunes {
		line = string(runes[:maxErrorRunes])
	}
	if line == "" {
		return "delivery failed"
	}
	return line
}
