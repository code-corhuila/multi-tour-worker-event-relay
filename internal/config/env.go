package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	runtimeconfig "github.com/code-corhuila/multi-tour-worker-runtime/config"
)

const defaultInterval = 5 * time.Second // ADR-004 D10

// Relay is everything the relay reads from the environment: the shared runtime variables
// plus the interval, the health address and the internal base URL of each service it talks to.
type Relay struct {
	Runtime          runtimeconfig.Config
	Interval         time.Duration
	HealthAddr       string
	SecurityURL      string
	IdentityAuditURL string
}

// LoadRelay reads the variables through getenv. Anything missing or malformed stops the worker at
// start; an error names the variable and never prints a value.
func LoadRelay(getenv func(string) string) (Relay, error) {
	shared, err := runtimeconfig.Load(getenv)
	if err != nil {
		return Relay{}, err
	}
	r := Relay{Runtime: shared, Interval: defaultInterval, HealthAddr: ":8080"}
	if s := getenv("RELAY_INTERVAL"); s != "" {
		if r.Interval, err = time.ParseDuration(s); err != nil || r.Interval <= 0 {
			return Relay{}, fmt.Errorf("RELAY_INTERVAL must be a positive duration")
		}
	}
	if s := getenv("HEALTH_ADDR"); s != "" {
		r.HealthAddr = s
	}
	if r.SecurityURL, err = baseURL(getenv, "SECURITY_BASE_URL"); err != nil {
		return Relay{}, err
	}
	if r.IdentityAuditURL, err = baseURL(getenv, "IDENTITY_AUDIT_BASE_URL"); err != nil {
		return Relay{}, err
	}
	return r, nil
}

func baseURL(getenv func(string) string, name string) (string, error) {
	raw := strings.TrimRight(getenv(name), "/")
	u, err := url.Parse(raw)
	// Only scheme://host[:port]: credentials, a query, a fragment or a path would reach every request.
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" {
		return "", fmt.Errorf("%s must be an http(s) url with only scheme, host and port", name)
	}
	return raw, nil
}
