package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/code-corhuila/multi-tour-worker-event-relay/internal/config"
)

func env(pairs ...string) func(string) string {
	m := map[string]string{}
	for i := 0; i < len(pairs); i += 2 {
		m[pairs[i]] = pairs[i+1]
	}
	return func(k string) string { return m[k] }
}

var required = []string{
	"SERVICE_TOKEN", "t",
	"SECURITY_BASE_URL", "http://security:8080",
	"IDENTITY_AUDIT_BASE_URL", "http://identity-audit-api:8080/",
}

func TestLoadRelay_applies_the_declared_defaults(t *testing.T) {
	got, err := config.LoadRelay(env(required...))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Interval != 5*time.Second || got.HealthAddr != ":8080" || got.Runtime.BatchSize != 100 {
		t.Errorf("defaults = %+v", got)
	}
	if got.SecurityURL != "http://security:8080" || got.IdentityAuditURL != "http://identity-audit-api:8080" {
		t.Errorf("urls = %q, %q", got.SecurityURL, got.IdentityAuditURL)
	}
}

func TestLoadRelay_reads_the_overrides(t *testing.T) {
	got, err := config.LoadRelay(env(append(required, "RELAY_INTERVAL", "2s", "HEALTH_ADDR", ":9090")...))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Interval != 2*time.Second || got.HealthAddr != ":9090" {
		t.Errorf("got = %+v", got)
	}
}

func TestLoadRelay_refuses_to_start_on_a_bad_value(t *testing.T) {
	cases := map[string][]string{
		"missing security url":       {"SERVICE_TOKEN", "t", "IDENTITY_AUDIT_BASE_URL", "http://a:1"},
		"missing identity-audit url": {"SERVICE_TOKEN", "t", "SECURITY_BASE_URL", "http://a:1"},
		"url without scheme":         append(append([]string{}, required...), "SECURITY_BASE_URL", "security:8080"),
		"missing token":              {"SECURITY_BASE_URL", "http://a:1", "IDENTITY_AUDIT_BASE_URL", "http://b:1"},
		"bad interval":               append(append([]string{}, required...), "RELAY_INTERVAL", "soon"),
		"zero interval":              append(append([]string{}, required...), "RELAY_INTERVAL", "0s"),
		"url with credentials":       append(append([]string{}, required...), "SECURITY_BASE_URL", "http://user:pw-9x@security:8086"),
		"url with a query":           append(append([]string{}, required...), "SECURITY_BASE_URL", "http://security:8086?token=pw-9x"),
		"url with a fragment":        append(append([]string{}, required...), "SECURITY_BASE_URL", "http://security:8086#pw-9x"),
		"url with a path":            append(append([]string{}, required...), "SECURITY_BASE_URL", "http://security:8086/pw-9x"),
	}
	for name, pairs := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := config.LoadRelay(env(pairs...))

			if err == nil || err.Error() == "not built" {
				t.Fatalf("want a validation error, got %v", err)
			}
			if strings.Contains(err.Error(), "SERVICE_TOKEN=") || strings.Contains(err.Error(), "pw-9x") {
				t.Errorf("error leaks a value: %v", err)
			}
		})
	}
}
