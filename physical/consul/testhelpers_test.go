package consul

import (
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// Environment contract for the live Consul tests.
//
//   - OPENBAO_CONSUL_TEST=1 turns "Consul unavailable" from a skip into a hard
//     failure. CI sets this so coverage is guaranteed and nothing is silently
//     skipped; a developer laptop leaves it unset and the live tests skip.
//   - CONSUL_HTTP_ADDR / CONSUL_HTTPS_ADDR override the endpoints (scheme is
//     stripped automatically). CONSUL_HTTP_TOKEN / CONSUL_CACERT are optional.
const (
	envConsulRequire = "OPENBAO_CONSUL_TEST"
	envConsulHTTP    = "CONSUL_HTTP_ADDR"
	envConsulHTTPS   = "CONSUL_HTTPS_ADDR"
	envConsulToken   = "CONSUL_HTTP_TOKEN"
	envConsulCACert  = "CONSUL_CACERT"

	defaultConsulHTTPAddr  = "127.0.0.1:8500"
	defaultConsulHTTPSAddr = "127.0.0.1:8501"
)

func consulRequired() bool { return os.Getenv(envConsulRequire) == "1" }

// stripScheme removes a leading http(s):// so a CONSUL_HTTP_ADDR like
// "http://host:8500" can be used directly as the backend "address".
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

func consulHTTPSAddr() string {
	if a := os.Getenv(envConsulHTTPS); a != "" {
		return stripScheme(a)
	}
	return defaultConsulHTTPSAddr
}

// probeTCP returns nil if addr accepts a TCP connection quickly. Used to decide
// skip-vs-fail before invoking NewConsulBackend (which otherwise retries for up
// to ~10s when Consul is absent).
func probeTCP(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	return c.Close()
}

// failOrSkip fails the test when OPENBAO_CONSUL_TEST=1, otherwise skips it.
func failOrSkip(t *testing.T, format string, args ...interface{}) {
	t.Helper()
	if consulRequired() {
		t.Fatalf(format, args...)
	}
	t.Skipf(format, args...)
}

// requireConsulReachable gates a test on a live Consul HTTP endpoint without
// returning a config map (for tests that build their own config maps).
func requireConsulReachable(t *testing.T) {
	t.Helper()
	addr := consulHTTPAddr()
	if err := probeTCP(addr); err != nil {
		failOrSkip(t, "Consul not reachable at %s (set %s=1 to require): %v", addr, envConsulRequire, err)
	}
}

// requireConsul gates a test needing a live Consul HTTP endpoint and returns the
// base config (address/path[/token]) for NewConsulBackend. When Consul is
// unreachable it hard-fails under OPENBAO_CONSUL_TEST=1, else skips.
func requireConsul(t *testing.T, path string) map[string]string {
	t.Helper()
	requireConsulReachable(t)
	cfg := map[string]string{"address": consulHTTPAddr(), "path": path}
	if tok := os.Getenv(envConsulToken); tok != "" {
		cfg["token"] = tok
	}
	return cfg
}

// requireConsulTLS gates a test needing a live Consul HTTPS endpoint and returns
// a TLS-enabled config. It only hard-fails (under OPENBAO_CONSUL_TEST=1) when
// CONSUL_HTTPS_ADDR is explicitly set; otherwise it always skips, so a plain
// dev-Consul CI (HTTP only) need not provision TLS.
func requireConsulTLS(t *testing.T, path string) map[string]string {
	t.Helper()
	addr := consulHTTPSAddr()
	if err := probeTCP(addr); err != nil {
		if consulRequired() && os.Getenv(envConsulHTTPS) != "" {
			t.Fatalf("Consul TLS not reachable at %s (%s set): %v", addr, envConsulHTTPS, err)
		}
		t.Skipf("Consul TLS not reachable at %s (set %s and %s=1 to require): %v", addr, envConsulHTTPS, envConsulRequire, err)
	}
	cfg := map[string]string{"address": addr, "path": path, "tls_enabled": "true"}
	if ca := os.Getenv(envConsulCACert); ca != "" {
		cfg["tls_ca_cert"] = ca
	} else {
		cfg["tls_skip_verify"] = "true"
	}
	if tok := os.Getenv(envConsulToken); tok != "" {
		cfg["token"] = tok
	}
	return cfg
}
