package out

import (
	"context"
	"encoding/json"
	"time"
)

// Event is one outbox event as a publisher exposes it (ADR-004 D4 envelope).
type Event struct {
	ID          string
	Type        string
	Version     int
	TenantID    string
	AggregateID string
	Payload     json.RawMessage
	OccurredAt  time.Time
}

// Route is where one event type goes: a consumer and its internal path.
// The path may hold the {tenantId} placeholder.
type Route struct {
	Consumer string
	Path     string
}

// Delivery is one event sent to one consumer route; IdempotencyKey is the event id.
type Delivery struct {
	Consumer       string
	Path           string
	IdempotencyKey string
	Event          Event
}

// EventSource is one publisher's outbox as the relay sees it. The relay never writes the table:
// the owning API does, behind these three internal routes.
type EventSource interface {
	// ListPending returns the due pending events, the one due for longest first.
	ListPending(ctx context.Context, page, limit int) ([]Event, error)
	// MarkPublished is called only after every consumer accepted the event.
	MarkPublished(ctx context.Context, eventID string) error
	// RecordFailedAttempt keeps a sanitized error; the owning API moves the retry time
	// and marks the event failed at the 20th attempt. ended says the owner answered that the
	// event is now FAILED.
	RecordFailedAttempt(ctx context.Context, eventID, message string) (ended bool, err error)
}

// EventSink delivers an event to a recipient's internal route. A nil error means 201 or 200.
type EventSink interface {
	Deliver(ctx context.Context, delivery Delivery) error
}
