package consul

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/sdk/v2/physical"
)

func TestConsulBackend_HA_BasicLocking(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping HA test in short mode")
	}

	var logger hclog.Logger
	if testing.Verbose() {
		logger = hclog.New(&hclog.LoggerOptions{
			Name:   "consul-ha-test",
			Level:  hclog.Debug,
			Output: os.Stdout,
		})
	} else {
		logger = hclog.NewNullLogger()
	}

	haConfig := requireConsul(t, "test/openbao/ha-basic/")
	haConfig["ha_enabled"] = "true"
	haConfig["advertise_addr"] = "http://127.0.0.1:8200"
	haConfig["session_ttl"] = "10s"
	haConfig["lock_delay"] = "5s"

	backend, err := NewConsulBackend(haConfig, logger)
	if err != nil {
		failOrSkip(t, "Consul not available for HA testing: %v", err)
	}

	haBackend, ok := backend.(physical.HABackend)
	if !ok {
		t.Fatal("Backend does not implement HABackend interface")
	}

	if !haBackend.HAEnabled() {
		t.Fatal("HA should be enabled")
	}

	// Test lock creation
	lock, err := haBackend.LockWith("test-lock", "test-value")
	if err != nil {
		t.Fatalf("Failed to create lock: %v", err)
	}

	// Test lock acquisition
	stopCh := make(chan struct{})
	defer close(stopCh)

	leaderCh, err := lock.Lock(stopCh)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// Verify we have the lock
	held, value, err := lock.Value()
	if err != nil {
		t.Fatalf("Failed to check lock value: %v", err)
	}

	if !held {
		t.Fatal("Lock should be held")
	}

	if value != "test-value" {
		t.Fatalf("Lock value mismatch: expected 'test-value', got '%s'", value)
	}

	// Verify leaderCh is open while we hold the lock
	select {
	case <-leaderCh:
		t.Fatal("Leader channel should not be closed while holding lock")
	case <-time.After(100 * time.Millisecond):
		// Good, channel is open
	}

	// Release the lock
	err = lock.Unlock()
	if err != nil {
		t.Fatalf("Failed to unlock: %v", err)
	}

	// Verify leader channel is closed after unlock
	select {
	case <-leaderCh:
		// Good, channel closed
	case <-time.After(5 * time.Second):
		t.Fatal("Leader channel should be closed after unlock")
	}

	// Verify lock is no longer held (with brief retry for eventual consistency)
	// var held bool
	for range 5 {
		held, _, err = lock.Value()
		if err != nil {
			t.Fatalf("Failed to check lock value after unlock: %v", err)
		}
		if !held {
			break // Success!
		}
		time.Sleep(100 * time.Millisecond)
	}
	if held {
		t.Fatal("Lock should not be held after unlock")
	}
}

func TestConsulBackend_HA_ConcurrentLocking(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping HA concurrent test in short mode")
	}

	var logger hclog.Logger
	if testing.Verbose() {
		logger = hclog.New(&hclog.LoggerOptions{
			Name:   "consul-ha-concurrent",
			Level:  hclog.Debug,
			Output: os.Stdout,
		})
	} else {
		logger = hclog.NewNullLogger()
	}

	haConfig := requireConsul(t, "test/openbao/ha-concurrent/")
	haConfig["ha_enabled"] = "true"
	haConfig["advertise_addr"] = "http://127.0.0.1:8200"
	haConfig["session_ttl"] = "10s"
	haConfig["lock_delay"] = "1s" // Shorter delay for faster tests

	// Create two backend instances
	backend1, err := NewConsulBackend(haConfig, logger.Named("backend1"))
	if err != nil {
		failOrSkip(t, "Consul not available: %v", err)
	}

	backend2, err := NewConsulBackend(haConfig, logger.Named("backend2"))
	if err != nil {
		t.Fatalf("Failed to create backend2: %v", err)
	}

	haBackend1 := backend1.(physical.HABackend)
	haBackend2 := backend2.(physical.HABackend)

	var wg sync.WaitGroup
	var results struct {
		sync.Mutex
		backend1Leader bool
		backend2Leader bool
		backend1Error  error
		backend2Error  error
	}

	lockKey := fmt.Sprintf("concurrent-test-%d", time.Now().UnixNano())

	// Try to acquire lock from both backends simultaneously
	wg.Add(2)

	// Backend 1 attempt
	go func() {
		defer wg.Done()

		lock, err := haBackend1.LockWith(lockKey, "backend1")
		if err != nil {
			results.Lock()
			results.backend1Error = err
			results.Unlock()
			return
		}

		stopCh := make(chan struct{})
		leaderCh, err := lock.Lock(stopCh)
		if err != nil {
			results.Lock()
			results.backend1Error = err
			results.Unlock()
			return
		}

		results.Lock()
		results.backend1Leader = true
		results.Unlock()

		// Hold lock for a bit
		time.Sleep(2 * time.Second)

		// Release lock
		_ = lock.Unlock()
		close(stopCh)

		// Wait for leadership loss signal
		<-leaderCh
	}()

	// Backend 2 attempt (start slightly later)
	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond) // Ensure backend1 tries first

		lock, err := haBackend2.LockWith(lockKey, "backend2")
		if err != nil {
			results.Lock()
			results.backend2Error = err
			results.Unlock()
			return
		}

		stopCh := make(chan struct{})
		defer close(stopCh)

		// This should initially fail or wait
		leaderCh, err := lock.Lock(stopCh)
		if err != nil {
			// This is expected - backend1 should have the lock
			t.Logf("Backend2 failed to acquire lock (expected): %v", err)
			return
		}

		// If we get here, we eventually acquired the lock
		results.Lock()
		results.backend2Leader = true
		results.Unlock()

		// Hold briefly then release
		time.Sleep(500 * time.Millisecond)
		_ = lock.Unlock()
		<-leaderCh
	}()

	wg.Wait()

	results.Lock()
	defer results.Unlock()

	// Check results
	if results.backend1Error != nil {
		t.Errorf("Backend1 error: %v", results.backend1Error)
	}

	// Only one should successfully become leader initially
	if !results.backend1Leader {
		t.Error("Backend1 should have acquired the lock first")
	}

	// Backend2 should either fail to acquire or acquire after backend1 releases
	t.Logf("Backend1 became leader: %v", results.backend1Leader)
	t.Logf("Backend2 became leader: %v", results.backend2Leader)
}

// Fixed test with better termination logic
func TestConsulBackend_HA_SessionRenewal(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping HA session renewal test in short mode")
	}

	var logger hclog.Logger
	if testing.Verbose() {
		logger = hclog.New(&hclog.LoggerOptions{
			Name:   "consul-ha-session",
			Level:  hclog.Debug,
			Output: os.Stdout,
		})
	} else {
		logger = hclog.NewNullLogger()
	}

	haConfig := requireConsul(t, "test/openbao/ha-session/")
	haConfig["ha_enabled"] = "true"
	haConfig["advertise_addr"] = "http://127.0.0.1:8200"
	haConfig["session_ttl"] = "12s" // Valid TTL (above 10s minimum)
	haConfig["lock_delay"] = "1s"

	backend, err := NewConsulBackend(haConfig, logger)
	if err != nil {
		failOrSkip(t, "Consul not available: %v", err)
	}

	haBackend := backend.(physical.HABackend)
	lock, err := haBackend.LockWith("session-test", "test-value")
	if err != nil {
		t.Fatalf("Failed to create lock: %v", err)
	}

	stopCh := make(chan struct{})
	leaderCh, err := lock.Lock(stopCh)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// We want to test that the lock survives across at least 2-3 renewal cycles
	// With 12s TTL, renewals happen every ~4s, so 25s should cover 6+ renewals
	testDuration := 25 * time.Second
	start := time.Now()

	// Check the lock periodically but terminate after testDuration
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	renewalChecks := 0
	testPassed := true

testLoop:
	for {
		select {
		case <-leaderCh:
			// If we lose leadership unexpectedly, that's a failure
			elapsed := time.Since(start)
			t.Errorf("Lock lost unexpectedly after %v (during renewal test)", elapsed)
			testPassed = false
			break testLoop

		case <-ticker.C:
			held, _, err := lock.Value()
			if err != nil {
				t.Errorf("Failed to check lock status: %v", err)
				testPassed = false
				break testLoop
			}
			if !held {
				t.Error("Lock should still be held during renewal period")
				testPassed = false
				break testLoop
			}

			renewalChecks++
			elapsed := time.Since(start)
			t.Logf("Lock still held after %v (check %d)", elapsed, renewalChecks)

			// If we've run long enough to test renewals, we can exit successfully
			if elapsed >= testDuration {
				t.Logf("Session renewal test completed successfully after %v (%d checks)", elapsed, renewalChecks)
				break testLoop
			}

		case <-time.After(testDuration + 10*time.Second):
			// Safety timeout in case something goes wrong
			t.Error("Test safety timeout reached")
			testPassed = false
			break testLoop
		}
	}

	// Clean up
	err = lock.Unlock()
	if err != nil {
		t.Errorf("Error during cleanup: %v", err)
	}

	// Verify the leader channel closes after unlock
	select {
	case <-leaderCh:
		t.Log("Leader channel properly closed after unlock")
	case <-time.After(5 * time.Second):
		t.Error("Leader channel should close after unlock")
		testPassed = false
	}

	if testPassed && renewalChecks >= 3 {
		t.Logf("✓ Session renewal test passed - lock held across %d checks over %v",
			renewalChecks, testDuration)
	} else if renewalChecks < 3 {
		t.Errorf("Test didn't run long enough to verify renewals (only %d checks)", renewalChecks)
	}
}

func TestConsulBackend_HA_LockContention(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping HA lock contention test in short mode")
	}

	var logger hclog.Logger
	if testing.Verbose() {
		logger = hclog.New(&hclog.LoggerOptions{
			Name:   "consul-ha-contention",
			Level:  hclog.Warn, // Reduce noise for this test
			Output: os.Stdout,
		})
	} else {
		logger = hclog.NewNullLogger()
	}

	haConfig := requireConsul(t, "test/openbao/ha-contention/")
	haConfig["ha_enabled"] = "true"
	haConfig["advertise_addr"] = "http://127.0.0.1:8200"
	haConfig["session_ttl"] = "10s"
	haConfig["lock_delay"] = "1s"

	numContenders := 5
	lockKey := fmt.Sprintf("contention-test-%d", time.Now().UnixNano())

	var successCount int64
	var errorCount int64
	// holders counts contenders inside the critical section at any instant;
	// exclusionViolations records every moment it exceeded one.
	var holders atomic.Int64
	var exclusionViolations atomic.Int64
	var wg sync.WaitGroup

	// Create multiple contenders
	for i := range numContenders {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			backend, err := NewConsulBackend(haConfig, logger.Named(fmt.Sprintf("contender%d", id)))
			if err != nil {
				atomic.AddInt64(&errorCount, 1)
				return
			}

			haBackend := backend.(physical.HABackend)
			lock, err := haBackend.LockWith(lockKey, fmt.Sprintf("contender-%d", id))
			if err != nil {
				atomic.AddInt64(&errorCount, 1)
				return
			}

			stopCh := make(chan struct{})
			leaderCh, err := lock.Lock(stopCh)
			if err != nil {
				// Lock blocks until the lock is free, so every contender is
				// expected to acquire it in turn; an error is a failure.
				atomic.AddInt64(&errorCount, 1)
				t.Errorf("Contender %d failed to acquire lock: %v", id, err)
				return
			}

			// We got the lock!
			atomic.AddInt64(&successCount, 1)
			if holders.Add(1) > 1 {
				exclusionViolations.Add(1)
			}
			t.Logf("Contender %d acquired the lock", id)

			// Hold it briefly
			time.Sleep(500 * time.Millisecond)

			// Release it
			holders.Add(-1)
			_ = lock.Unlock()
			close(stopCh)
			<-leaderCh

			t.Logf("Contender %d released the lock", id)
		}(i)
	}

	wg.Wait()

	// The invariant a lock must uphold is mutual exclusion: never two holders
	// at once. It is NOT "only one contender ever acquires" -- Lock blocks
	// until the lock is free, so each contender takes it in turn.
	if v := exclusionViolations.Load(); v != 0 {
		t.Errorf("Lock granted to more than one contender at a time (%d violations)", v)
	}
	if successCount != int64(numContenders) {
		t.Errorf("Expected all %d contenders to acquire the lock in turn, got %d", numContenders, successCount)
	}
	if errorCount != 0 {
		t.Errorf("Expected no acquisition errors, got %d", errorCount)
	}

	t.Logf("Lock contention test completed: %d successful, %d errors out of %d contenders",
		successCount, errorCount, numContenders)
}

func TestConsulBackend_HA_LockValue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping HA lock value test in short mode")
	}

	logger := hclog.NewNullLogger()

	haConfig := requireConsul(t, "test/openbao/ha-value/")
	haConfig["ha_enabled"] = "true"
	haConfig["advertise_addr"] = "http://127.0.0.1:8200"

	backend, err := NewConsulBackend(haConfig, logger)
	if err != nil {
		failOrSkip(t, "Consul not available: %v", err)
	}

	haBackend := backend.(physical.HABackend)
	lockKey := fmt.Sprintf("value-test-%d", time.Now().UnixNano())
	testValue := "test-lock-value-12345"

	// Test when lock doesn't exist
	lock, err := haBackend.LockWith(lockKey, testValue)
	if err != nil {
		t.Fatalf("Failed to create lock: %v", err)
	}

	held, value, err := lock.Value()
	if err != nil {
		t.Fatalf("Failed to check non-existent lock: %v", err)
	}
	if held {
		t.Fatal("Lock should not be held initially")
	}
	if value != "" {
		t.Fatalf("Expected empty value for non-existent lock, got '%s'", value)
	}

	// Acquire the lock
	stopCh := make(chan struct{})
	leaderCh, err := lock.Lock(stopCh)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// Test while lock is held
	held, value, err = lock.Value()
	if err != nil {
		t.Fatalf("Failed to check held lock: %v", err)
	}
	if !held {
		t.Fatal("Lock should be held")
	}
	if value != testValue {
		t.Fatalf("Expected value '%s', got '%s'", testValue, value)
	}

	// Release and verify
	_ = lock.Unlock()
	close(stopCh)
	<-leaderCh

	held, _, err = lock.Value()
	if err != nil {
		t.Fatalf("Failed to check released lock: %v", err)
	}
	if held {
		t.Fatal("Lock should not be held after release")
	}
}

func TestConsulBackend_HA_Configuration(t *testing.T) {
	// Every case below expects success, and NewConsulBackend performs a
	// connection test during construction, so this needs a live Consul.
	requireConsulReachable(t)

	logger := hclog.NewNullLogger()

	testCases := []struct {
		name      string
		config    map[string]string
		shouldErr bool
		errMsg    string
		// expectHA states the expected HAEnabled() rather than deriving it
		// from the config string, so a case using a non-canonical spelling
		// cannot silently assert the opposite of the contract.
		expectHA bool
	}{
		{
			name: "valid HA config",
			config: map[string]string{
				"address":        consulHTTPAddr(),
				"path":           "test/",
				"ha_enabled":     "true",
				"advertise_addr": "http://127.0.0.1:8200",
			},
			shouldErr: false,
			expectHA:  true,
		},
		{
			// HA now defaults to on, matching Vault, so disabling it takes an
			// explicit value.
			name: "HA disabled",
			config: map[string]string{
				"address":    consulHTTPAddr(),
				"path":       "test/",
				"ha_enabled": "false",
			},
			shouldErr: false,
			expectHA:  false,
		},
		{
			name: "HA defaults to enabled when unset",
			config: map[string]string{
				"address": consulHTTPAddr(),
				"path":    "test/",
			},
			shouldErr: false,
			expectHA:  true,
		},
		{
			name: "unparseable ha_enabled is rejected",
			config: map[string]string{
				"address":    consulHTTPAddr(),
				"path":       "test/",
				"ha_enabled": "yes-please",
			},
			shouldErr: true,
			errMsg:    "invalid ha_enabled",
		},
		{
			name: "unparseable session_ttl is rejected",
			config: map[string]string{
				"address":     consulHTTPAddr(),
				"path":        "test/",
				"session_ttl": "30 fortnights",
			},
			shouldErr: true,
			errMsg:    "invalid session_ttl",
		},
		{
			name: "unparseable lock_delay is rejected",
			config: map[string]string{
				"address":    consulHTTPAddr(),
				"path":       "test/",
				"lock_delay": "soon",
			},
			shouldErr: true,
			errMsg:    "invalid lock_delay",
		},
		{
			name: "negative lock_delay is rejected",
			config: map[string]string{
				"address":    consulHTTPAddr(),
				"path":       "test/",
				"lock_delay": "-5s",
			},
			shouldErr: true,
			errMsg:    "cannot be negative",
		},
		{
			// Consul reads an absent delay as its own default, so 0 would
			// quietly mean 15s rather than "no delay".
			name: "zero lock_delay is rejected",
			config: map[string]string{
				"address":    consulHTTPAddr(),
				"path":       "test/",
				"lock_delay": "0s",
			},
			shouldErr: true,
			errMsg:    "not supported",
		},
		{
			// ParseBool takes strconv's set, which the old string compare
			// did not: "1" worked but "True" and "t" did not.
			name: "non-canonical true spelling enables HA",
			config: map[string]string{
				"address":    consulHTTPAddr(),
				"path":       "test/",
				"ha_enabled": "True",
			},
			shouldErr: false,
			expectHA:  true,
		},
		{
			name: "non-canonical false spelling disables HA",
			config: map[string]string{
				"address":    consulHTTPAddr(),
				"path":       "test/",
				"ha_enabled": "0",
			},
			shouldErr: false,
			expectHA:  false,
		},
		{
			name: "custom HA timing",
			config: map[string]string{
				"address":        consulHTTPAddr(),
				"path":           "test/",
				"ha_enabled":     "true",
				"advertise_addr": "http://127.0.0.1:8200",
				"session_ttl":    "30s",
				"lock_delay":     "10s",
			},
			shouldErr: false,
			expectHA:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			backend, err := NewConsulBackend(tc.config, logger)

			if tc.shouldErr {
				if err == nil {
					t.Fatalf("Expected error containing '%s'", tc.errMsg)
				}
				if tc.errMsg != "" && !contains(err.Error(), tc.errMsg) {
					t.Fatalf("Expected error containing '%s', got '%s'", tc.errMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if haBackend, ok := backend.(physical.HABackend); ok {
				if haBackend.HAEnabled() != tc.expectHA {
					t.Fatalf("Expected HA enabled: %v, got: %v", tc.expectHA, haBackend.HAEnabled())
				}
			}
		})
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > len(substr) && (s[:len(substr)] == substr ||
			s[len(s)-len(substr):] == substr ||
			findSubstring(s, substr))))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestConsulBackend_HA_SDKConformance runs the SDK's HA contract suite against
// two independent backends sharing one Consul, the same way raft, postgresql
// and inmem do.
//
// Consul was the only HA backend in the tree not wired into this suite, which
// is how two physical.Lock contract violations survived: Value() reported "held
// by me" rather than "held by any node" (leaving Core.LeaderLocked unable to
// find the active node), and Lock() returned an error instead of blocking when
// the lock was already held. ExerciseHABackend asserts both directly.
func TestConsulBackend_HA_SDKConformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping HA conformance test in short mode")
	}

	logger := hclog.NewNullLogger()

	// Both backends must share one prefix so they contend for the same key;
	// the timestamp keeps reruns from inheriting a previous run's state.
	path := fmt.Sprintf("test/openbao/ha-conformance-%d/", time.Now().UnixNano())

	newBackend := func() physical.HABackend {
		t.Helper()
		cfg := requireConsul(t, path)
		cfg["ha_enabled"] = "true"
		b, err := NewConsulBackend(cfg, logger)
		if err != nil {
			failOrSkip(t, "Consul not available: %v", err)
		}
		ha, ok := b.(physical.HABackend)
		if !ok {
			t.Fatal("consul backend does not implement physical.HABackend")
		}
		return ha
	}

	physical.ExerciseHABackend(t, newBackend(), newBackend())
}

// TestConsulBackend_HA_LegacyLockKeyReclaim covers the compatibility path for
// lock keys written before the backend moved to the Consul lock helper.
//
// That earlier implementation acquired the key with no flags. The helper marks
// its own keys with api.LockFlagValue and rejects any key without it, and a
// released Consul session leaves the key behind, so an upgraded node would
// otherwise fail every acquisition with "Existing key does not match lock use"
// and could never take leadership. Reclaiming is only safe for a key that
// nothing holds and that is not already a lock, which is what these cases pin
// down.
func TestConsulBackend_HA_LegacyLockKeyReclaim(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping HA legacy reclaim test in short mode")
	}

	logger := hclog.NewNullLogger()
	path := fmt.Sprintf("test/openbao/ha-legacy-%d/", time.Now().UnixNano())

	newBackend := func() *ConsulBackend {
		t.Helper()
		cfg := requireConsul(t, path)
		cfg["ha_enabled"] = "true"
		b, err := NewConsulBackend(cfg, logger)
		if err != nil {
			failOrSkip(t, "Consul not available: %v", err)
		}
		return b.(*ConsulBackend)
	}

	backend := newBackend()

	// writeLegacyKey reproduces exactly what the old implementation wrote: a
	// KV entry with no flags, optionally held by a session.
	writeLegacyKey := func(t *testing.T, key string, withSession bool) string {
		t.Helper()
		pair := &api.KVPair{Key: backend.consulKey(key), Value: []byte("legacy-holder")}
		var sessionID string
		if withSession {
			var err error
			// Nothing renews this session, so it must outlive the subtest:
			// were it to lapse mid-test the key would silently become the
			// unheld case and the assertions below would prove nothing. The
			// assertions run in well under a second; the TTL is generous only
			// to keep that true if the subtest ever grows.
			sessionID, _, err = backend.client.Session().Create(&api.SessionEntry{
				Name:     "openbao-legacy-test",
				TTL:      "120s",
				Behavior: api.SessionBehaviorRelease,
			}, nil)
			if err != nil {
				t.Fatalf("failed to create legacy session: %v", err)
			}
			t.Cleanup(func() { _, _ = backend.client.Session().Destroy(sessionID, nil) })
			pair.Session = sessionID
			acquired, _, err := backend.kv.Acquire(pair, nil)
			if err != nil || !acquired {
				t.Fatalf("failed to seed held legacy key: acquired=%v err=%v", acquired, err)
			}
		} else if _, err := backend.kv.Put(pair, nil); err != nil {
			t.Fatalf("failed to seed legacy key: %v", err)
		}

		seeded, _, err := backend.kv.Get(backend.consulKey(key), nil)
		if err != nil || seeded == nil {
			t.Fatalf("legacy key not seeded: %v", err)
		}
		if seeded.Flags != 0 {
			t.Fatalf("seeded key should carry no flags, got %d", seeded.Flags)
		}
		return sessionID
	}

	t.Run("unheld legacy key is reclaimed", func(t *testing.T) {
		key := fmt.Sprintf("unheld-%d", time.Now().UnixNano())
		writeLegacyKey(t, key, false)

		lock, err := backend.LockWith(key, "new-holder")
		if err != nil {
			t.Fatalf("LockWith failed: %v", err)
		}

		stopCh := make(chan struct{})
		leaderCh, err := lock.Lock(stopCh)
		if err != nil {
			t.Fatalf("Lock should reclaim the legacy key and succeed, got: %v", err)
		}
		if leaderCh == nil {
			t.Fatal("expected a leader channel after acquiring the lock")
		}
		defer func() { _ = lock.Unlock() }()

		// The key must now be a well-formed Consul lock owned by this node.
		pair, _, err := backend.kv.Get(backend.consulKey(key), nil)
		if err != nil || pair == nil {
			t.Fatalf("lock key missing after acquire: %v", err)
		}
		if pair.Flags != api.LockFlagValue {
			t.Errorf("reclaimed key should carry LockFlagValue, got %d", pair.Flags)
		}
		if pair.Session == "" {
			t.Error("reclaimed key should be held by a session")
		}
		if string(pair.Value) != "new-holder" {
			t.Errorf("expected value new-holder, got %q", pair.Value)
		}
	})

	t.Run("legacy key held by a session is left alone", func(t *testing.T) {
		key := fmt.Sprintf("held-%d", time.Now().UnixNano())
		sessionID := writeLegacyKey(t, key, true)

		lock, err := backend.LockWith(key, "new-holder")
		if err != nil {
			t.Fatalf("LockWith failed: %v", err)
		}

		// Something holds this key, so it must not be deleted. Acquisition
		// cannot succeed either: the helper refuses a key it did not mark, and
		// reports that as soon as it reads the key. stopCh is a safety net so
		// a regression that starts waiting cannot hang the test rather than
		// the mechanism under test.
		stopCh := make(chan struct{})
		timer := time.AfterFunc(30*time.Second, func() { close(stopCh) })
		defer timer.Stop()

		leaderCh, err := lock.Lock(stopCh)
		if err == nil {
			t.Error("expected acquisition against a held legacy key to fail")
		} else if !errors.Is(err, api.ErrLockConflict) {
			t.Errorf("expected ErrLockConflict, got: %v", err)
		}
		if leaderCh != nil {
			t.Error("must not gain leadership over a key another session holds")
		}

		pair, _, err := backend.kv.Get(backend.consulKey(key), nil)
		if err != nil {
			t.Fatalf("failed to read key: %v", err)
		}
		if pair == nil {
			t.Fatal("a held legacy key must never be deleted")
		}
		if pair.Session != sessionID {
			t.Errorf("holder session changed: want %q, got %q", sessionID, pair.Session)
		}
		if string(pair.Value) != "legacy-holder" {
			t.Errorf("holder value changed: got %q", pair.Value)
		}
	})
}

// TestConsulBackend_HA_LockDelayCoversMonitorTolerance pins the safety margin
// that makes the lock delay default load-bearing.
//
// The lock monitor tolerates lockMonitorRetries transient Consul errors,
// lockMonitorRetryTime apart, before it declares leadership lost. A former
// leader can therefore still believe it is active for that long after its
// session died. Consul's lock delay is what stops a successor from acquiring
// and writing inside that window, and it is the only thing that does: unlike
// upstream Vault's Consul backend, this one does not implement
// physical.FencingHABackend, so its writes carry no session check.
//
// Shortening the delay below the monitor's tolerance would let two nodes write
// to the same barrier concurrently after an unclean leader loss. Needs no
// Consul: it is a relationship between constants, which is exactly why it is
// worth pinning.
func TestConsulBackend_HA_LockDelayCoversMonitorTolerance(t *testing.T) {
	tolerance := time.Duration(lockMonitorRetries) * lockMonitorRetryTime
	if defaultLockDelay < tolerance {
		t.Fatalf("default lock delay %s is shorter than the %s the lock monitor spends retrying before it reports leadership lost; a demoted leader could still be writing when a successor takes over",
			defaultLockDelay, tolerance)
	}

	// Zero is rejected rather than silently becoming Consul's own default, so
	// whatever the default is must stay expressible.
	if defaultLockDelay <= 0 {
		t.Fatalf("default lock delay %s is not expressible: Consul reads an absent delay as its own default", defaultLockDelay)
	}

	// Consul refuses a lock delay above 60s.
	if defaultLockDelay > 60*time.Second {
		t.Fatalf("default lock delay %s exceeds the 60s Consul accepts", defaultLockDelay)
	}
}

// TestConsulBackend_HA_LockDelayConfigurable checks an explicitly configured
// delay reaches the backend, so a deployment can trade the margin above for
// faster failover if it accepts the risk.
func TestConsulBackend_HA_LockDelayConfigurable(t *testing.T) {
	requireConsulReachable(t)

	cfg := requireConsul(t, "test/openbao/ha-lockdelay/")
	cfg["ha_enabled"] = "true"

	b, err := NewConsulBackend(cfg, hclog.NewNullLogger())
	if err != nil {
		failOrSkip(t, "Consul not available: %v", err)
	}
	if got := b.(*ConsulBackend).lockDelay; got != defaultLockDelay {
		t.Errorf("expected the default lock delay %s, got %s", defaultLockDelay, got)
	}

	cfg["lock_delay"] = "5s"
	b2, err := NewConsulBackend(cfg, hclog.NewNullLogger())
	if err != nil {
		t.Fatalf("failed to build backend with an explicit lock_delay: %v", err)
	}
	if got := b2.(*ConsulBackend).lockDelay; got != 5*time.Second {
		t.Errorf("expected the configured lock delay 5s, got %s", got)
	}
}
