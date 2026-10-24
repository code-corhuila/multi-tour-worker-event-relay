package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/code-corhuila/multi-tour-worker-runtime/health"
	"github.com/code-corhuila/multi-tour-worker-runtime/httpclient"
	"github.com/code-corhuila/multi-tour-worker-runtime/logging"
	"github.com/code-corhuila/multi-tour-worker-runtime/runner"

	relayhttp "github.com/code-corhuila/multi-tour-worker-event-relay/internal/adapter/out/http"
	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/application/usecase"
	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/config"
)

// The health server's limits are declared here, not left to defaults (Annex D).
const (
	readHeaderTimeout = 5 * time.Second
	writeTimeout      = 15 * time.Second
	idleTimeout       = 60 * time.Second
	shutdownTimeout   = 20 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "event-relay:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadRelay(os.Getenv)
	if err != nil {
		return err
	}
	table, err := config.NewRoutingTable(config.DefaultRoutes())
	if err != nil {
		return err
	}
	log := logging.New(os.Stdout)

	client := func(baseURL string) *httpclient.Client {
		return httpclient.New(baseURL, cfg.Runtime.ServiceToken,
			httpclient.WithTimeout(cfg.Runtime.HTTPTimeout), httpclient.WithAttempts(cfg.Runtime.HTTPAttempts))
	}
	security, identityAudit := client(cfg.SecurityURL), client(cfg.IdentityAuditURL)
	consumers := map[string]*httpclient.Client{"security": security, "identity-audit": identityAudit}
	for _, name := range table.Consumers() {
		if consumers[name] == nil {
			return fmt.Errorf("the routing table delivers to %q, which has no client", name)
		}
	}
	job := usecase.NewRelayEvents(
		[]usecase.Publisher{
			{Name: "identity-audit", Source: relayhttp.NewOutbox(identityAudit, "/api/v1/identity-audit/internal/outbox")},
			{Name: "security", Source: relayhttp.NewOutbox(security, "/api/v1/security/internal/outbox")},
		},
		table,
		relayhttp.NewSink(consumers),
		log,
	)
	jobRunner, err := runner.New(job, runner.Options{Interval: cfg.Interval, BatchSize: cfg.Runtime.BatchSize,
		RunTimeout: cfg.Runtime.RunTimeout, Logger: log})
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("GET /health", health.Handler())
	server := &http.Server{Addr: cfg.HealthAddr, Handler: mux, ReadHeaderTimeout: readHeaderTimeout,
		WriteTimeout: writeTimeout, IdleTimeout: idleTimeout}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()
	runnerDone := make(chan struct{})
	go func() { jobRunner.Start(ctx); close(runnerDone) }()

	var failure error
	select {
	case <-ctx.Done():
	case failure = <-serverErr:
		stop()
	}
	// Ordered shutdown: the run in flight ends first (bounded by RUN_TIMEOUT), then the health server.
	<-runnerDone
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil && failure == nil {
		failure = err
	}
	if errors.Is(failure, http.ErrServerClosed) {
		return nil
	}
	return failure
}
