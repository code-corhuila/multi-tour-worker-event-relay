package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"
	"unicode"

	workerruntime "github.com/code-corhuila/multi-tour-worker-runtime"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/port/out"
)

const (
	pageLimit         = 100
	maxErrorRunes     = 500
	stuckAfter        = 5 * time.Minute // a pending event older than this raises an alert (ADR-004 D8)
	tenantPlaceholder = "{tenantId}"
)

// uuidPattern is strict on purpose: ids come from the publisher and become path segments and a header.
var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

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
	now        func() time.Time
}

var _ workerruntime.Job = (*RelayEvents)(nil)

// NewRelayEvents wires the job.
func NewRelayEvents(publishers []Publisher, router Router, sink out.EventSink, log *slog.Logger) *RelayEvents {
	return &RelayEvents{publishers: publishers, router: router, sink: sink, log: log, now: time.Now}
}

// WithClock sets the clock the age of a pending event is measured with.
func (r *RelayEvents) WithClock(now func() time.Time) *RelayEvents {
	r.now = now
	return r
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
			log.WarnContext(ctx, "reading the outbox failed, left for the next run")
			return processed, failed + 1
		}
		fresh := events[:0:0]
		for _, e := range events {
			if !seen[e.ID] {
				seen[e.ID] = true
				fresh = append(fresh, e)
				if age := r.now().Sub(e.OccurredAt); !e.OccurredAt.IsZero() && age > stuckAfter {
					log.WarnContext(ctx, "pending event older than five minutes", "alert", "event_pending_too_long",
						"eventId", e.ID, "eventType", clean(e.Type), "age", age.Round(time.Second).String())
				}
			}
		}
		if len(fresh) == 0 {
			return processed, failed
		}
		p, f := workerruntime.Each(ctx, log, fresh, func(ctx context.Context, e out.Event) error {
			return r.relay(ctx, publisher, e)
		})
		processed, failed = processed+p, failed+f
		budget -= len(fresh)
	}
	return processed, failed
}

// relay delivers one event to all its routes. A failure is recorded in the source and returned as
// an error whose text is the sanitized message, because the runtime logs what the handler returns.
// If MarkPublished fails no attempt is recorded, on purpose: the event is still pending, is
// redelivered next run with the same Idempotency-Key and consumers apply it once. The cost is that
// such a run does not move the event toward the 20 attempts, so a source that can never mark an
// event shows only in the failed count and the log.
func (r *RelayEvents) relay(ctx context.Context, publisher Publisher, e out.Event) error {
	source := publisher.Source
	if !uuidPattern.MatchString(e.ID) {
		// The id is a path segment of the source: an invalid one cannot even be recorded against.
		return errors.New("event has an invalid id")
	}
	routes, known := r.router.RoutesFor(e.Type)
	if !known {
		return r.fail(ctx, publisher, e, fmt.Sprintf("unknown event type %q", clean(e.Type)))
	}
	var failures []string
	for _, route := range routes {
		if reason := r.deliver(ctx, route, e); reason != "" {
			failures = append(failures, route.Consumer+": "+reason)
		}
	}
	if len(failures) > 0 {
		return r.fail(ctx, publisher, e, strings.Join(failures, "; "))
	}
	return source.MarkPublished(ctx, e.ID)
}

// deliver returns "" on success or a fixed reason: the raw error of a sink can hold hosts or
// credentials, so it never reaches the source or the log.
func (r *RelayEvents) deliver(ctx context.Context, route out.Route, e out.Event) string {
	path := route.Path
	if strings.Contains(path, tenantPlaceholder) {
		if !uuidPattern.MatchString(e.TenantID) {
			return "event has an invalid tenantId"
		}
		path = strings.ReplaceAll(path, tenantPlaceholder, e.TenantID)
	}
	if err := r.sink.Deliver(ctx, out.Delivery{Consumer: route.Consumer, Path: path, IdempotencyKey: e.ID, Event: e}); err != nil {
		return "delivery failed"
	}
	return ""
}

// fail records the attempt and returns the same sanitized message as the error. When the owner answers that the
// event is now FAILED it raises the alert; the payload is never part of the line.
func (r *RelayEvents) fail(ctx context.Context, publisher Publisher, e out.Event, message string) error {
	message = clean(message)
	ended, err := publisher.Source.RecordFailedAttempt(ctx, e.ID, message)
	if err != nil {
		return errors.New(message + "; recording the attempt failed")
	}
	if ended {
		r.log.ErrorContext(ctx, "event failed after its attempts", "alert", "event_failed",
			"eventId", e.ID, "eventType", clean(e.Type), "publisher", publisher.Name)
	}
	return errors.New(message)
}

// clean makes text one printable line of bounded length, as the attempts route requires.
func clean(text string) string {
	line := strings.Join(strings.FieldsFunc(text, func(c rune) bool { return unicode.IsControl(c) || unicode.IsSpace(c) }), " ")
	if runes := []rune(line); len(runes) > maxErrorRunes {
		line = string(runes[:maxErrorRunes])
	}
	if line == "" {
		return "delivery failed"
	}
	return line
}
