package usecase_test

import (
	"context"
	"errors"
	"testing"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/port/out"
	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/config"
)

// Relay spec 009, AC-RL-09.4: with the real table, a fact of a reset that job 4 refuses stays pending and is delivered again to both consumers
// with the same Idempotency-Key, so the audit applies it once; it is published only when both accept.
func TestRun_a_failed_notifications_delivery_leaves_the_fact_pending(t *testing.T) {
	rt, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		t.Fatalf("default table: %v", err)
	}
	src := newSource(event(id1, "PasswordReset"))
	sink := &fakeSink{failFor: func(d out.Delivery) error {
		if d.Consumer == "notifications" {
			return errors.New("notifications answered 503")
		}
		return nil
	}}

	processed, failed := relay(src, rt, sink).Run(context.Background(), 10)

	if processed != 0 || failed != 1 || src.published[id1] {
		t.Fatalf("processed=%d failed=%d published=%v, want the fact left pending", processed, failed, src.published)
	}
	if len(sink.to("identity-audit")) != 1 || len(sink.to("notifications")) != 1 {
		t.Fatalf("deliveries = %+v", sink.deliveries)
	}
	sink.failFor = nil
	if processed, _ = relay(src, rt, sink).Run(context.Background(), 10); processed != 1 || !src.published[id1] {
		t.Fatalf("processed=%d published=%v, want it published once both accept", processed, src.published)
	}
	audit := sink.to("identity-audit")
	if len(audit) != 2 || audit[0].IdempotencyKey != audit[1].IdempotencyKey || audit[0].IdempotencyKey == "" {
		t.Fatalf("the audit got %+v, want two deliveries with one key", audit)
	}
}
