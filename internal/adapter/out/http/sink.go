package http

import (
	"context"

	"github.com/code-corhuila/multi-tour-worker-runtime/httpclient"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/port/out"
)

// Sink delivers events to the consumer named in the delivery, one client per consumer.
type Sink struct {
	clients map[string]*httpclient.Client
}

var _ out.EventSink = (*Sink)(nil)

// NewSink maps each consumer name to the client of its base URL.
func NewSink(clients map[string]*httpclient.Client) *Sink { return &Sink{clients: clients} }

// Deliver is not built yet.
func (s *Sink) Deliver(context.Context, out.Delivery) error { return nil }
