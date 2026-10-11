package http

import (
	"context"

	"github.com/code-corhuila/multi-tour-worker-runtime/httpclient"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/port/out"
)

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

// ListPending is not built yet.
func (o *Outbox) ListPending(context.Context, int, int) ([]out.Event, error) { return nil, nil }

// MarkPublished is not built yet.
func (o *Outbox) MarkPublished(context.Context, string) error { return nil }

// RecordFailedAttempt is not built yet.
func (o *Outbox) RecordFailedAttempt(context.Context, string, string) error { return nil }
