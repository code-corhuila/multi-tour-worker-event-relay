package http

import (
	"fmt"

	"github.com/code-corhuila/multi-tour-worker-runtime/httpclient"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/usecase"
)

// publisherNames are the services whose outbox the relay reads, in the order it reads them (relay spec 007 adds commercial-reservation; G-RL7-1 adds cash-reporting).
// Each outbox is at /api/v1/<service>/internal/outbox, the three routes every publisher offers (ADR-004 D4).
var publisherNames = []string{"identity-audit", "security", "operations-cost", "commercial-reservation", "cash-reporting"}

// DefaultPublishers builds the outboxes to read, one per service, over the client of each. A service without a client stops the worker at
// start, so a forgotten line is found there and not at the first run.
func DefaultPublishers(clients map[string]*httpclient.Client) ([]usecase.Publisher, error) {
	publishers := make([]usecase.Publisher, 0, len(publisherNames))
	for _, name := range publisherNames {
		client := clients[name]
		if client == nil {
			return nil, fmt.Errorf("the outbox of %q is read, but it has no client", name)
		}
		publishers = append(publishers, usecase.Publisher{Name: name, Source: NewOutbox(client, "/api/v1/"+name+"/internal/outbox")})
	}
	return publishers, nil
}
