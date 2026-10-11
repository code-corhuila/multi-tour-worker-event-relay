package http

import (
	"context"
	"fmt"
	"net/http"

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

// Deliver posts the event envelope to the consumer; 201 and 200 both count as accepted.
func (s *Sink) Deliver(ctx context.Context, d out.Delivery) error {
	client, ok := s.clients[d.Consumer]
	if !ok {
		return fmt.Errorf("no base url for consumer %q", d.Consumer)
	}
	req := httpclient.Request{Method: http.MethodPost, Path: d.Path, Body: envelopeOf(d.Event), IdempotencyKey: d.IdempotencyKey}
	if err := client.Do(ctx, req, nil); err != nil {
		return fmt.Errorf("deliver to %s: %w", d.Consumer, err)
	}
	return nil
}
