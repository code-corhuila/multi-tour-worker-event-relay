package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/code-corhuila/multi-tour-worker-runtime/httpclient"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/port/out"
)

// envelope is the event as ADR-004 D4 writes it: what a publisher lists and a consumer receives.
// Fields the relay does not name (status, attempts, ...) are ignored when reading.
type envelope struct {
	ID          string          `json:"eventId"`
	Type        string          `json:"eventType"`
	Version     int             `json:"eventVersion"`
	TenantID    string          `json:"tenantId,omitempty"`
	AggregateID string          `json:"aggregateId"`
	Payload     json.RawMessage `json:"payload"`
	OccurredAt  time.Time       `json:"occurredAt"`
}

func (e envelope) event() out.Event {
	return out.Event{ID: e.ID, Type: e.Type, Version: e.Version, TenantID: e.TenantID,
		AggregateID: e.AggregateID, Payload: e.Payload, OccurredAt: e.OccurredAt}
}

func envelopeOf(e out.Event) envelope {
	return envelope{ID: e.ID, Type: e.Type, Version: e.Version, TenantID: e.TenantID,
		AggregateID: e.AggregateID, Payload: e.Payload, OccurredAt: e.OccurredAt}
}

// Outbox reads and updates one publisher's outbox through its internal routes.
type Outbox struct {
	client   *httpclient.Client
	basePath string
}

var _ out.EventSource = (*Outbox)(nil)

// NewOutbox returns the outbox behind basePath, e.g. /api/v1/security/internal/outbox.
func NewOutbox(client *httpclient.Client, basePath string) *Outbox {
	return &Outbox{client: client, basePath: basePath}
}

// ListPending reads one page of the due pending events.
func (o *Outbox) ListPending(ctx context.Context, page, limit int) ([]out.Event, error) {
	query := url.Values{"status": {"PENDING"}, "page": {strconv.Itoa(page)}, "limit": {strconv.Itoa(limit)}}
	result, err := httpclient.List[envelope](ctx, o.client, o.basePath+"?"+query.Encode())
	if err != nil {
		return nil, fmt.Errorf("list pending events: %w", err)
	}
	events := make([]out.Event, len(result.Data))
	for i, e := range result.Data {
		events[i] = e.event()
	}
	return events, nil
}

// MarkPublished tells the owning API that every consumer accepted the event.
func (o *Outbox) MarkPublished(ctx context.Context, eventID string) error {
	req := httpclient.Request{Method: http.MethodPost, Path: o.eventPath(eventID, "published"),
		IdempotencyKey: eventID + ":published"} // a repeat answers 200, so the runtime may retry it
	if err := o.client.Do(ctx, req, nil); err != nil {
		return fmt.Errorf("mark event published: %w", err)
	}
	return nil
}

// RecordFailedAttempt gives the owning API a sanitized error; it moves the retry time itself.
func (o *Outbox) RecordFailedAttempt(ctx context.Context, eventID, message string) (bool, error) {
	req := httpclient.Request{Method: http.MethodPost, Path: o.eventPath(eventID, "attempts"),
		Body: map[string]string{"error": message}}
	var answer struct {
		Status string `json:"status"`
	}
	err := o.client.Do(ctx, req, &answer)
	var unusable *httpclient.DecodeError
	switch {
	case errors.As(err, &unusable):
		return false, nil // a 2xx: the owner counted the attempt; only its answer cannot be read
	case err != nil:
		return false, fmt.Errorf("record failed attempt: %w", err)
	}
	return answer.Status == "FAILED", nil
}

func (o *Outbox) eventPath(eventID, action string) string {
	return o.basePath + "/" + url.PathEscape(eventID) + "/" + action
}
