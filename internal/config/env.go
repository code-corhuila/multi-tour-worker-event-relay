package config

import (
	"errors"
	"time"

	runtimeconfig "github.com/code-corhuila/multi-tour-worker-runtime/config"
)

// Relay is everything the relay reads from the environment: the shared runtime variables
// plus the interval, the health address and the internal base URL of each service it talks to.
type Relay struct {
	Runtime          runtimeconfig.Config
	Interval         time.Duration
	HealthAddr       string
	SecurityURL      string
	IdentityAuditURL string
}

// LoadRelay is not built yet.
func LoadRelay(func(string) string) (Relay, error) { return Relay{}, errors.New("not built") }
