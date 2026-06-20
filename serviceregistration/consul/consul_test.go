package consul

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/serviceregistration"
)

// -----------------------------------------------------------------------------
// Tier A: pure config parsing (no live Consul required, always runs)
// -----------------------------------------------------------------------------

func TestConsulServiceRegistration_ParseConfig_Defaults(t *testing.T) {
	cfg, err := ParseServiceRegistrationConfig(map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.ServiceName != defaultServiceName {
		t.Fatalf("expected default service name %q, got %q", defaultServiceName, cfg.ServiceName)
	}
}

func TestConsulServiceRegistration_ParseConfig_NilMap(t *testing.T) {
	if _, err := ParseServiceRegistrationConfig(nil); err == nil {
		t.Fatal("expected error for nil config map")
	}
}

func TestConsulServiceRegistration_ParseConfig_Disabled(t *testing.T) {
	cfg, err := ParseServiceRegistrationConfig(map[string]string{"disable_registration": "true"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatalf("expected nil config when registration disabled, got %+v", cfg)
	}
}

func TestConsulServiceRegistration_ParseConfig_BadBool(t *testing.T) {
	if _, err := ParseServiceRegistrationConfig(map[string]string{"disable_registration": "notabool"}); err == nil {
		t.Fatal("expected error for invalid disable_registration value")
	}
}

func TestConsulServiceRegistration_ParseConfig_Tags(t *testing.T) {
	cfg, err := ParseServiceRegistrationConfig(map[string]string{"service_tags": "a, b ,c"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"a", "b", "c"}
	if len(cfg.ServiceTags) != len(want) {
		t.Fatalf("expected %v, got %v", want, cfg.ServiceTags)
	}
	for i, tag := range want {
		if cfg.ServiceTags[i] != tag {
			t.Fatalf("tag %d: expected %q, got %q", i, tag, cfg.ServiceTags[i])
		}
	}
}

func TestConsulServiceRegistration_ParseConfig_PortAndRetries(t *testing.T) {
	cfg, err := ParseServiceRegistrationConfig(map[string]string{
		"service_port": "8200",
		"max_retries":  "5",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ServicePort != 8200 {
		t.Fatalf("expected service_port 8200, got %d", cfg.ServicePort)
	}
	if cfg.MaxRetries != 5 {
		t.Fatalf("expected max_retries 5, got %d", cfg.MaxRetries)
	}

	if _, err := ParseServiceRegistrationConfig(map[string]string{"service_port": "notanint"}); err == nil {
		t.Fatal("expected error for invalid service_port")
	}
}

func TestConsulServiceRegistration_ParseConfig_TLS(t *testing.T) {
	cfg, err := ParseServiceRegistrationConfig(map[string]string{
		"tls_enabled":     "true",
		"tls_skip_verify": "true",
		"tls_ca_cert":     "/path/to/ca.pem",
		"tls_server_name": "consul.example.com",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.TLSConfig == nil {
		t.Fatal("expected non-nil TLSConfig when tls_enabled=true")
	}
	if !cfg.TLSConfig.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify true")
	}
	if cfg.TLSConfig.CAFile != "/path/to/ca.pem" {
		t.Fatalf("expected CAFile to be set, got %q", cfg.TLSConfig.CAFile)
	}
	if cfg.TLSConfig.ServerName != "consul.example.com" {
		t.Fatalf("expected ServerName to be set, got %q", cfg.TLSConfig.ServerName)
	}
}

func TestConsulServiceRegistration_Notify_NoOps(t *testing.T) {
	// Construction does not connect to Consul (lazy client), and an explicit
	// service_address avoids local-address autodetection, so this needs no Consul.
	reg, err := NewConsulServiceRegistration(map[string]string{
		"service":         "openbao",
		"service_address": "127.0.0.1",
		"service_port":    "8200",
	}, hclog.NewNullLogger(), serviceregistration.State{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := reg.NotifyActiveStateChange(true); err != nil {
		t.Fatalf("NotifyActiveStateChange: %v", err)
	}
	if err := reg.NotifySealedStateChange(false); err != nil {
		t.Fatalf("NotifySealedStateChange: %v", err)
	}
	if err := reg.NotifyPerformanceStandbyStateChange(false); err != nil {
		t.Fatalf("NotifyPerformanceStandbyStateChange: %v", err)
	}
	if err := reg.NotifyInitializedStateChange(true); err != nil {
		t.Fatalf("NotifyInitializedStateChange: %v", err)
	}
}

func TestConsulServiceRegistration_Disabled_RunReturnsImmediately(t *testing.T) {
	reg, err := NewConsulServiceRegistration(map[string]string{"disable_registration": "true"},
		hclog.NewNullLogger(), serviceregistration.State{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	shutdownCh := make(chan struct{})
	defer close(shutdownCh)
	var wg sync.WaitGroup
	wg.Add(1)
	if err := reg.Run(shutdownCh, &wg, ""); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	// Disabled path calls wait.Done() before returning, so this does not block.
	wg.Wait()
}

// -----------------------------------------------------------------------------
// Tier B: live registration round-trip (real Consul agent, gated)
// -----------------------------------------------------------------------------

func TestConsulServiceRegistration_Register_Live(t *testing.T) {
	cfg := requireConsul(t)

	name := fmt.Sprintf("openbao-test-%d", time.Now().UnixNano())
	port := freeTCPPort(t)
	cfg["service"] = name
	cfg["service_address"] = "127.0.0.1"
	cfg["service_port"] = strconv.Itoa(port)
	cfg["service_tags"] = "openbao,test"

	reg, err := NewConsulServiceRegistration(cfg, hclog.NewNullLogger(), serviceregistration.State{})
	if err != nil {
		t.Fatalf("failed to create service registration: %v", err)
	}

	client := verifyClient(t)

	shutdownCh := make(chan struct{})
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(func() { close(shutdownCh) }) }
	var wg sync.WaitGroup
	wg.Add(1)
	// Backstop: always shut down and deregister even if an assertion fails.
	t.Cleanup(func() {
		stop()
		wg.Wait()
		_ = client.Agent().ServiceDeregister(name)
	})

	if err := reg.Run(shutdownCh, &wg, ""); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	// Registration happens in a background goroutine — poll until it appears.
	waitForService(t, client, name, true)

	// Verify the registered service's attributes.
	svcs, err := client.Agent().Services()
	if err != nil {
		t.Fatalf("failed to list agent services: %v", err)
	}
	var found bool
	for _, s := range svcs {
		if s.Service != name {
			continue
		}
		found = true
		if s.Port != port {
			t.Errorf("expected port %d, got %d", port, s.Port)
		}
		if s.Meta["version"] == "" {
			t.Errorf("expected version meta to be set")
		}
		if !containsString(s.Tags, "test") {
			t.Errorf("expected tag 'test' in %v", s.Tags)
		}
	}
	if !found {
		t.Fatalf("service %q not found after registration", name)
	}

	// Shut down and confirm deregistration.
	stop()
	wg.Wait()
	waitForService(t, client, name, false)
}

func containsString(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}
