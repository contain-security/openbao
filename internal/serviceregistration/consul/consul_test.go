package consul

import (
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/v2/internal/serviceregistration"
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

func TestConsulServiceRegistration_Notify_UpdatesState(t *testing.T) {
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

	csr, ok := reg.(*consulServiceRegistration)
	if !ok {
		t.Fatalf("expected *consulServiceRegistration, got %T", reg)
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

	// The notifications must actually be recorded; they were stubs that
	// discarded the value, which left every node advertised with whatever
	// tags it started with.
	csr.mu.Lock()
	state := csr.state
	csr.mu.Unlock()

	if !state.IsActive || state.IsSealed || state.IsPerformanceStandby || !state.IsInitialized {
		t.Fatalf("state not recorded from notifications: %+v", state)
	}

	tags := csr.serviceTags(state)
	if !containsString(tags, tagActive) {
		t.Errorf("expected %q in tags %v", tagActive, tags)
	}
	if containsString(tags, tagStandby) {
		t.Errorf("did not expect %q in tags %v", tagStandby, tags)
	}
	if got := checkStatus(state); got != api.HealthPassing {
		t.Errorf("expected an unsealed node to be passing, got %q", got)
	}

	// A burst of notifications must not block Core, even with nothing
	// draining the channel (Run was never called here).
	for range 10 {
		if err := reg.NotifyActiveStateChange(false); err != nil {
			t.Fatalf("NotifyActiveStateChange under burst: %v", err)
		}
	}
}

func TestConsulServiceRegistration_ServiceTags(t *testing.T) {
	reg, err := NewConsulServiceRegistration(map[string]string{
		"service":         "openbao",
		"service_address": "127.0.0.1",
		"service_port":    "8200",
		"service_tags":    "static-one,static-two",
	}, hclog.NewNullLogger(), serviceregistration.State{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	csr, ok := reg.(*consulServiceRegistration)
	if !ok {
		t.Fatalf("expected *consulServiceRegistration, got %T", reg)
	}

	cases := []struct {
		name       string
		state      serviceregistration.State
		wantTags   []string
		absentTags []string
		wantStatus string
	}{
		{
			name:       "active and unsealed",
			state:      serviceregistration.State{IsActive: true, IsInitialized: true},
			wantTags:   []string{tagActive, tagInitialized},
			absentTags: []string{tagStandby, tagSealed, tagPerformanceStandby},
			wantStatus: api.HealthPassing,
		},
		{
			name:       "standby and unsealed",
			state:      serviceregistration.State{IsInitialized: true},
			wantTags:   []string{tagStandby, tagInitialized},
			absentTags: []string{tagActive, tagSealed},
			wantStatus: api.HealthPassing,
		},
		{
			// A sealed node can serve nothing, so it must fall out of the
			// healthy set a load balancer draws from.
			name:       "sealed",
			state:      serviceregistration.State{IsSealed: true, IsInitialized: true},
			wantTags:   []string{tagStandby, tagSealed, tagInitialized},
			absentTags: []string{tagActive},
			wantStatus: api.HealthCritical,
		},
		{
			name:       "performance standby",
			state:      serviceregistration.State{IsPerformanceStandby: true, IsInitialized: true},
			wantTags:   []string{tagStandby, tagPerformanceStandby, tagInitialized},
			absentTags: []string{tagActive},
			wantStatus: api.HealthPassing,
		},
		{
			name:       "uninitialized",
			state:      serviceregistration.State{},
			wantTags:   []string{tagStandby},
			absentTags: []string{tagInitialized, tagActive, tagSealed},
			wantStatus: api.HealthPassing,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tags := csr.serviceTags(tc.state)
			for _, want := range tc.wantTags {
				if !containsString(tags, want) {
					t.Errorf("expected %q in %v", want, tags)
				}
			}
			for _, absent := range tc.absentTags {
				if containsString(tags, absent) {
					t.Errorf("did not expect %q in %v", absent, tags)
				}
			}
			// Statically configured tags survive alongside the state tags.
			for _, static := range []string{"static-one", "static-two"} {
				if !containsString(tags, static) {
					t.Errorf("expected configured tag %q in %v", static, tags)
				}
			}
			if got := checkStatus(tc.state); got != tc.wantStatus {
				t.Errorf("expected check status %q, got %q", tc.wantStatus, got)
			}
		})
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

	// The caller's WaitGroup is supplied un-Added, exactly as Core supplies
	// it, and the disabled path starts nothing, so it must neither Add nor
	// Done: Wait returns immediately on a zero counter.
	var wg sync.WaitGroup
	if err := reg.Run(shutdownCh, &wg, ""); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
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
	// Run registers its own goroutine with the WaitGroup, so the caller must
	// not Add on its behalf.
	var wg sync.WaitGroup
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
	return slices.Contains(haystack, needle)
}

// TestConsulServiceRegistration_StateChange_Live proves the notifications reach
// Consul: a node re-tags itself on a leadership change and its health check
// tracks seal state.
//
// Without this the notifications were stubs, so a node kept whatever tags it
// registered with. It advertised itself as healthy while sealed, and never
// gained the "active" tag, so active.<service>.service.consul resolved to
// nothing and a tag-routing load balancer sent traffic to a node that could
// not serve it.
func TestConsulServiceRegistration_StateChange_Live(t *testing.T) {
	cfg := requireConsul(t)

	name := fmt.Sprintf("openbao-state-%d", time.Now().UnixNano())
	port := freeTCPPort(t)
	cfg["service"] = name
	cfg["service_address"] = "127.0.0.1"
	cfg["service_port"] = strconv.Itoa(port)
	// Keep the refresh interval short so the test does not wait on the default.
	cfg["check_timeout"] = "1s"

	// Start sealed and uninitialized, which is how a node comes up.
	reg, err := NewConsulServiceRegistration(cfg, hclog.NewNullLogger(),
		serviceregistration.State{IsSealed: true})
	if err != nil {
		t.Fatalf("failed to create service registration: %v", err)
	}

	client := verifyClient(t)

	shutdownCh := make(chan struct{})
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(func() { close(shutdownCh) }) }
	var wg sync.WaitGroup
	t.Cleanup(func() {
		stop()
		wg.Wait()
		_ = client.Agent().ServiceDeregister(name)
	})

	if err := reg.Run(shutdownCh, &wg, ""); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	waitForService(t, client, name, true)

	// Sealed: tagged sealed and standby, and the check must not be passing.
	awaitTags(t, client, name, []string{tagSealed, tagStandby}, []string{tagActive})
	awaitCheckStatus(t, client, name, api.HealthCritical)

	// Unseal and take leadership, the transition that matters for routing.
	if err := reg.NotifyInitializedStateChange(true); err != nil {
		t.Fatalf("NotifyInitializedStateChange: %v", err)
	}
	if err := reg.NotifySealedStateChange(false); err != nil {
		t.Fatalf("NotifySealedStateChange: %v", err)
	}
	if err := reg.NotifyActiveStateChange(true); err != nil {
		t.Fatalf("NotifyActiveStateChange: %v", err)
	}

	awaitTags(t, client, name, []string{tagActive, tagInitialized}, []string{tagStandby, tagSealed})
	awaitCheckStatus(t, client, name, api.HealthPassing)

	// Step down again: the node must stop advertising itself as active.
	if err := reg.NotifyActiveStateChange(false); err != nil {
		t.Fatalf("NotifyActiveStateChange: %v", err)
	}
	awaitTags(t, client, name, []string{tagStandby}, []string{tagActive})

	stop()
	wg.Wait()
	waitForService(t, client, name, false)
}

// awaitTags polls until the service carries every tag in want and none in
// absent. Registration updates run on a background goroutine, so the catalog
// lags the notification slightly.
func awaitTags(t *testing.T, client *api.Client, name string, want, absent []string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last []string
	for time.Now().Before(deadline) {
		svcs, err := client.Agent().Services()
		if err != nil {
			t.Fatalf("failed to list agent services: %v", err)
		}
		for _, s := range svcs {
			if s.Service != name {
				continue
			}
			last = s.Tags
			ok := true
			for _, w := range want {
				if !containsString(s.Tags, w) {
					ok = false
				}
			}
			for _, a := range absent {
				if containsString(s.Tags, a) {
					ok = false
				}
			}
			if ok {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for tags want=%v absent=%v, last saw %v", want, absent, last)
}

// awaitCheckStatus polls until the service's health check reaches status.
func awaitCheckStatus(t *testing.T, client *api.Client, name, status string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		checks, err := client.Agent().Checks()
		if err != nil {
			t.Fatalf("failed to list agent checks: %v", err)
		}
		for _, c := range checks {
			if c.ServiceName != name {
				continue
			}
			last = c.Status
			if c.Status == status {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if last == "" {
		t.Fatalf("no health check registered for service %q", name)
	}
	t.Fatalf("timed out waiting for check status %q, last saw %q", status, last)
}

// TestConsulServiceRegistration_Run_DoesNotAddToCallersWaitGroup guards the
// contract Core actually uses: internal/command/server.go builds one shared
// WaitGroup, never calls Add on it, and only Waits. An implementation that
// calls Done on it panics with "sync: negative WaitGroup counter". Every other
// test here calls Add itself, which hid that.
func TestConsulServiceRegistration_Run_DoesNotAddToCallersWaitGroup(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		reg, err := NewConsulServiceRegistration(map[string]string{"disable_registration": "true"},
			hclog.NewNullLogger(), serviceregistration.State{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		shutdownCh := make(chan struct{})
		defer close(shutdownCh)

		// Fresh WaitGroup, exactly as Core supplies it.
		var wg sync.WaitGroup
		if err := reg.Run(shutdownCh, &wg, ""); err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
		wg.Wait()
	})

	t.Run("enabled", func(t *testing.T) {
		// Deliberately unroutable. This subtest is about the WaitGroup
		// contract, not about reaching Consul, and the ids are deterministic
		// now -- against the default loopback agent it would have shared an
		// id with, and so deregistered, a real OpenBao running locally.
		reg, err := NewConsulServiceRegistration(map[string]string{
			"address":         "127.0.0.1:1",
			"service":         "openbao",
			"service_address": "127.0.0.1",
			"service_port":    "8200",
			"max_retries":     "1",
		}, hclog.NewNullLogger(), serviceregistration.State{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		shutdownCh := make(chan struct{})
		var wg sync.WaitGroup
		if err := reg.Run(shutdownCh, &wg, ""); err != nil {
			t.Fatalf("Run returned error: %v", err)
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		// Wait must not return while the goroutine is still running. Checking
		// only after shutdown would pass just as happily against a bare "go
		// fn()" that never registered with the caller's WaitGroup at all,
		// which is the third way to get this wrong and the one a passing
		// Wait cannot otherwise distinguish. The address is unroutable, so
		// the maintenance loop provably keeps running until shutdown.
		select {
		case <-done:
			t.Fatal("wg.Wait returned before shutdown: Run did not register its goroutine with the caller's WaitGroup")
		case <-time.After(250 * time.Millisecond):
		}

		// And it must return once shutdown completes, so Core actually waits
		// for deregistration rather than exiting from under it.
		close(shutdownCh)
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for the registration goroutine to finish")
		}
	})
}

func TestConsulServiceRegistration_ParseConfig_CheckTimeout(t *testing.T) {
	cases := []struct {
		name    string
		value   string
		absent  bool
		want    time.Duration
		wantErr bool
	}{
		{name: "absent uses the default", absent: true, want: defaultCheckTimeout},
		{name: "duration string", value: "30s", want: 30 * time.Second},
		{name: "bare seconds", value: "30", want: 30 * time.Second},
		{name: "at the minimum", value: "1s", want: minCheckTimeout},
		{name: "at the maximum", value: "5m", want: maxCheckTimeout},
		{name: "unparseable is rejected", value: "notaduration", wantErr: true},
		{name: "below the minimum is rejected", value: "500ms", wantErr: true},
		{name: "zero is rejected", value: "0", wantErr: true},
		{name: "above the maximum is rejected", value: "10m", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := map[string]string{"service": "openbao", "service_port": "8200"}
			if !tc.absent {
				cfg["check_timeout"] = tc.value
			}

			got, err := ParseServiceRegistrationConfig(cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error for check_timeout=%q", tc.value)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.CheckTimeout != tc.want {
				t.Errorf("expected check_timeout %s, got %s", tc.want, got.CheckTimeout)
			}
		})
	}
}

// TestConsulServiceRegistration_CheckTTL pins the TTL comfortably above a
// single stalled refresh: a slow agent must not be able to expire the check of
// a node that is perfectly healthy.
func TestConsulServiceRegistration_CheckTTL(t *testing.T) {
	for _, interval := range []time.Duration{minCheckTimeout, defaultCheckTimeout, maxCheckTimeout} {
		reg, err := NewConsulServiceRegistration(map[string]string{
			"service":         "openbao",
			"service_address": "127.0.0.1",
			"service_port":    "8200",
			"check_timeout":   interval.String(),
		}, hclog.NewNullLogger(), serviceregistration.State{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		csr, ok := reg.(*consulServiceRegistration)
		if !ok {
			t.Fatalf("expected *consulServiceRegistration, got %T", reg)
		}

		ttl := csr.checkTTL()
		if ttl <= interval+consulHTTPTimeout {
			t.Errorf("check_timeout %s: TTL %s leaves no room for a refresh that stalls for the full HTTP timeout %s",
				interval, ttl, consulHTTPTimeout)
		}
	}
}

// TestConsulServiceRegistration_RegisterBackoff pins the retry schedule and,
// more importantly, that it stays positive and bounded for any max_retries an
// operator might set. An unbounded shift overflows the duration negative,
// which fires the timer immediately and turns the retry into a hot loop.
func TestConsulServiceRegistration_RegisterBackoff(t *testing.T) {
	// The cap is only lossless while a full shift still exceeds the ceiling.
	if got := time.Duration(1<<maxBackoffShift) * time.Second; got < maxRegisterBackoff {
		t.Fatalf("maxBackoffShift yields %s, which truncates the progression below maxRegisterBackoff %s", got, maxRegisterBackoff)
	}

	want := []time.Duration{2, 4, 8, 16, 30, 30}
	for i, w := range want {
		expected := w*time.Second + 100*time.Millisecond
		if got := registerBackoff(i); got != expected {
			t.Errorf("attempt %d: expected %s, got %s", i, expected, got)
		}
	}

	// Anything an operator could plausibly configure, plus well beyond it.
	for _, attempt := range []int{6, 34, 62, 63, 1000} {
		got := registerBackoff(attempt)
		if got <= 0 {
			t.Errorf("attempt %d: backoff must stay positive, got %s", attempt, got)
		}
		if got > maxRegisterBackoff+100*time.Millisecond {
			t.Errorf("attempt %d: backoff %s exceeds the ceiling %s", attempt, got, maxRegisterBackoff)
		}
	}
}
