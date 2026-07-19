package consul

import (
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
)

// Environment contract mirrors the physical/consul tests:
//   - OPENBAO_CONSUL_TEST=1 turns "Consul unavailable" from a skip into a hard
//     failure (CI sets this so live coverage is guaranteed).
//   - CONSUL_HTTP_ADDR overrides the endpoint; CONSUL_HTTP_TOKEN is optional.
const (
	envConsulRequire = "OPENBAO_CONSUL_TEST"
	envConsulHTTP    = "CONSUL_HTTP_ADDR"
	envConsulToken   = "CONSUL_HTTP_TOKEN"

	defaultConsulHTTPAddr = "127.0.0.1:8500"
)

func consulRequired() bool { return os.Getenv(envConsulRequire) == "1" }

func stripScheme(a string) string {
	if i := strings.Index(a, "://"); i >= 0 {
		return a[i+3:]
	}
	return a
}

func consulHTTPAddr() string {
	if a := os.Getenv(envConsulHTTP); a != "" {
		return stripScheme(a)
	}
	return defaultConsulHTTPAddr
}

func probeTCP(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	return c.Close()
}

func failOrSkip(t *testing.T, format string, args ...interface{}) {
	t.Helper()
	if consulRequired() {
		t.Fatalf(format, args...)
	}
	t.Skipf(format, args...)
}

// requireConsul gates a test needing a live Consul HTTP endpoint and returns the
// base service_registration config (address[/token]). Hard-fails under
// OPENBAO_CONSUL_TEST=1 when Consul is unreachable, else skips.
func requireConsul(t *testing.T) map[string]string {
	t.Helper()
	addr := consulHTTPAddr()
	if err := probeTCP(addr); err != nil {
		failOrSkip(t, "Consul not reachable at %s (set %s=1 to require): %v", addr, envConsulRequire, err)
	}
	cfg := map[string]string{"address": addr}
	if tok := os.Getenv(envConsulToken); tok != "" {
		cfg["token"] = tok
	}
	return cfg
}

// verifyClient returns a Consul API client pointed at the test endpoint, used to
// assert that a service was actually registered/deregistered in the agent.
func verifyClient(t *testing.T) *api.Client {
	t.Helper()
	cfg := api.DefaultConfig()
	cfg.Address = consulHTTPAddr()
	if tok := os.Getenv(envConsulToken); tok != "" {
		cfg.Token = tok
	}
	client, err := api.NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create Consul verify client: %v", err)
	}
	return client
}

// freeTCPPort returns an OS-assigned free TCP port for use as a service port.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find a free port: %v", err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// serviceRegistered reports whether a service with the given name is present in
// the local Consul agent.
func serviceRegistered(t *testing.T, client *api.Client, name string) bool {
	t.Helper()
	svcs, err := client.Agent().Services()
	if err != nil {
		t.Fatalf("failed to list agent services: %v", err)
	}
	for _, s := range svcs {
		if s.Service == name {
			return true
		}
	}
	return false
}

// waitForService polls until serviceRegistered(name) == want, or fails on timeout.
func waitForService(t *testing.T, client *api.Client, name string, want bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if serviceRegistered(t, client, name) == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for service %q registered==%v", name, want)
}
