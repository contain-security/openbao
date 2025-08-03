package consul

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

	haConfig := map[string]string{
		"address":        "127.0.0.1:8500",
		"path":           "test/openbao/ha-basic/",
		"ha_enabled":     "true",
		"advertise_addr": "http://127.0.0.1:8200",
		"session_ttl":    "10s",
		"lock_delay":     "5s",
	}

	backend, err := NewConsulBackend(haConfig, logger)
	if err != nil {
		t.Skipf("Consul not available for HA testing: %v", err)
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
	for i := 0; i < 5; i++ {
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

	haConfig := map[string]string{
		"address":        "127.0.0.1:8500",
		"path":           "test/openbao/ha-concurrent/",
		"ha_enabled":     "true",
		"advertise_addr": "http://127.0.0.1:8200",
		"session_ttl":    "10s",
		"lock_delay":     "1s", // Shorter delay for faster tests
	}

	// Create two backend instances
	backend1, err := NewConsulBackend(haConfig, logger.Named("backend1"))
	if err != nil {
		t.Skipf("Consul not available: %v", err)
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
		lock.Unlock()
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
		lock.Unlock()
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

	haConfig := map[string]string{
		"address":        "127.0.0.1:8500",
		"path":           "test/openbao/ha-session/",
		"ha_enabled":     "true",
		"advertise_addr": "http://127.0.0.1:8200",
		"session_ttl":    "12s", // Valid TTL (above 10s minimum)
		"lock_delay":     "1s",
	}

	backend, err := NewConsulBackend(haConfig, logger)
	if err != nil {
		t.Skipf("Consul not available: %v", err)
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

	haConfig := map[string]string{
		"address":        "127.0.0.1:8500",
		"path":           "test/openbao/ha-contention/",
		"ha_enabled":     "true",
		"advertise_addr": "http://127.0.0.1:8200",
		"session_ttl":    "10s",
		"lock_delay":     "1s",
	}

	numContenders := 5
	lockKey := fmt.Sprintf("contention-test-%d", time.Now().UnixNano())

	var successCount int64
	var errorCount int64
	var wg sync.WaitGroup

	// Create multiple contenders
	for i := 0; i < numContenders; i++ {
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
				// Expected for most contenders
				t.Logf("Contender %d failed to acquire lock: %v", id, err)
				return
			}

			// We got the lock!
			atomic.AddInt64(&successCount, 1)
			t.Logf("Contender %d acquired the lock", id)

			// Hold it briefly
			time.Sleep(500 * time.Millisecond)

			// Release it
			lock.Unlock()
			close(stopCh)
			<-leaderCh

			t.Logf("Contender %d released the lock", id)
		}(i)
	}

	wg.Wait()

	// Exactly one should have succeeded
	if successCount != 1 {
		t.Errorf("Expected exactly 1 successful lock acquisition, got %d", successCount)
	}

	t.Logf("Lock contention test completed: %d successful, %d errors out of %d contenders",
		successCount, errorCount, numContenders)
}

func TestConsulBackend_HA_LockValue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping HA lock value test in short mode")
	}

	logger := hclog.NewNullLogger()

	haConfig := map[string]string{
		"address":        "127.0.0.1:8500",
		"path":           "test/openbao/ha-value/",
		"ha_enabled":     "true",
		"advertise_addr": "http://127.0.0.1:8200",
	}

	backend, err := NewConsulBackend(haConfig, logger)
	if err != nil {
		t.Skipf("Consul not available: %v", err)
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
	lock.Unlock()
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
	logger := hclog.NewNullLogger()

	testCases := []struct {
		name      string
		config    map[string]string
		shouldErr bool
		errMsg    string
	}{
		{
			name: "valid HA config",
			config: map[string]string{
				"address":        "127.0.0.1:8500",
				"path":           "test/",
				"ha_enabled":     "true",
				"advertise_addr": "http://127.0.0.1:8200",
			},
			shouldErr: false,
		},
		{
			name: "HA disabled",
			config: map[string]string{
				"address": "127.0.0.1:8500",
				"path":    "test/",
			},
			shouldErr: false,
		},
		{
			name: "custom HA timing",
			config: map[string]string{
				"address":        "127.0.0.1:8500",
				"path":           "test/",
				"ha_enabled":     "true",
				"advertise_addr": "http://127.0.0.1:8200",
				"session_ttl":    "30s",
				"lock_delay":     "10s",
			},
			shouldErr: false,
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
				expectedHA := tc.config["ha_enabled"] == "true"
				if haBackend.HAEnabled() != expectedHA {
					t.Fatalf("Expected HA enabled: %v, got: %v", expectedHA, haBackend.HAEnabled())
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
