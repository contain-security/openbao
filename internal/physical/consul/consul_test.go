package consul

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/sdk/v2/physical"
)

func TestConsulBackend_BASIC_CRUD(t *testing.T) {
	logger := hclog.NewNullLogger()

	backend, err := NewConsulBackend(requireConsul(t, "test/openbao/crud/"), logger)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	ctx := context.Background()

	// Test Put and Get
	entry := &physical.Entry{
		Key:   "test/key1",
		Value: []byte("test-value-1"),
	}

	err = backend.Put(ctx, entry)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	retrieved, err := backend.Get(ctx, "test/key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved == nil {
		t.Fatal("Get returned nil for existing key")
	}

	if retrieved.Key != entry.Key {
		t.Fatalf("Key mismatch: expected %s, got %s", entry.Key, retrieved.Key)
	}

	if string(retrieved.Value) != string(entry.Value) {
		t.Fatalf("Value mismatch: expected %s, got %s", entry.Value, retrieved.Value)
	}

	// Test Get non-existent key
	nonExistent, err := backend.Get(ctx, "test/nonexistent")
	if err != nil {
		t.Fatalf("Get failed for non-existent key: %v", err)
	}

	if nonExistent != nil {
		t.Fatal("Get should return nil for non-existent key")
	}

	// Test Delete
	err = backend.Delete(ctx, "test/key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion
	deleted, err := backend.Get(ctx, "test/key1")
	if err != nil {
		t.Fatalf("Get failed after delete: %v", err)
	}

	if deleted != nil {
		t.Fatal("Key should be nil after deletion")
	}

	// Test Delete non-existent (should not error)
	err = backend.Delete(ctx, "test/nonexistent")
	if err != nil {
		t.Fatalf("Delete should not error for non-existent key: %v", err)
	}
}

func TestConsulBackend_BASIC_List(t *testing.T) {
	logger := hclog.NewNullLogger()

	backend, err := NewConsulBackend(requireConsul(t, "test/openbao/list/"), logger)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	ctx := context.Background()

	// Clean up any existing test data
	_ = backend.Delete(ctx, "list-test/key1")
	_ = backend.Delete(ctx, "list-test/key2")
	_ = backend.Delete(ctx, "list-test/subdir/key3")

	// Create test data
	testEntries := []*physical.Entry{
		{Key: "list-test/key1", Value: []byte("value1")},
		{Key: "list-test/key2", Value: []byte("value2")},
		{Key: "list-test/subdir/key3", Value: []byte("value3")},
	}

	for _, entry := range testEntries {
		err = backend.Put(ctx, entry)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Test listing
	keys, err := backend.List(ctx, "list-test/")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	expectedKeys := []string{"key1", "key2", "subdir/"}
	if len(keys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d: %v", len(expectedKeys), len(keys), keys)
	}

	// Clean up
	for _, entry := range testEntries {
		_ = backend.Delete(ctx, entry.Key)
	}
}

func TestConsulBackend_BASIC_ConcurrentOperations(t *testing.T) {
	logger := hclog.NewNullLogger()

	backend, err := NewConsulBackend(requireConsul(t, "test/openbao/concurrent/"), logger)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	ctx := context.Background()

	// Test concurrent writes
	done := make(chan bool, 10)

	for i := range 10 {
		go func(id int) {
			defer func() { done <- true }()

			entry := &physical.Entry{
				Key:   fmt.Sprintf("concurrent/key%d", id),
				Value: fmt.Appendf(nil, "value%d", id),
			}

			err := backend.Put(ctx, entry)
			if err != nil {
				t.Errorf("Concurrent put failed for key%d: %v", id, err)
				return
			}

			retrieved, err := backend.Get(ctx, entry.Key)
			if err != nil {
				t.Errorf("Concurrent get failed for key%d: %v", id, err)
				return
			}

			if string(retrieved.Value) != string(entry.Value) {
				t.Errorf("Concurrent value mismatch for key%d", id)
			}
		}(i)
	}

	// Wait for all goroutines
	timeout := time.After(10 * time.Second)
	for range 10 {
		select {
		case <-done:
			// Good
		case <-timeout:
			t.Fatal("Concurrent test timed out")
		}
	}

	// Clean up
	for i := range 10 {
		_ = backend.Delete(ctx, fmt.Sprintf("concurrent/key%d", i))
	}
}

// Helper to clean up ListPage test data
func cleanupListPageData(backend *ConsulBackend, ctx context.Context, prefix string, t *testing.T) {
	keys, err := backend.List(ctx, prefix)
	if err != nil {
		t.Logf("Warning: Failed to list keys for cleanup: %v", err)
		return
	}
	for _, key := range keys {
		fullKey := prefix + key
		if err := backend.Delete(ctx, fullKey); err != nil {
			t.Logf("Warning: Failed to delete key %s during cleanup: %v", fullKey, err)
		}
	}
}

// TestConsulBackend_ListPage_Pattern1_SimplePagination tests the simple pagination pattern (after/limit)
func TestConsulBackend_BASIC_ListPage_Pattern1_SimplePagination(t *testing.T) {
	logger := hclog.NewNullLogger()
	// logger := hclog.New(&hclog.LoggerOptions{
	//	Name:   "consul-test",
	//	Level:  hclog.Trace, // Maximum verbosity
	//	Output: os.Stderr,   // Use stderr to separate from test output
	//	Color:  hclog.AutoColor})

	backend, err := NewConsulBackend(requireConsul(t, "test/openbao/listpage/"), logger)
	if err != nil {
		t.Fatalf("Failed to create backend: %v", err)
	}

	ctx := context.Background()
	testPrefix := "listpage-simple-test/"

	// Clean up any existing test data
	cleanupListPageData(backend.(*ConsulBackend), ctx, testPrefix, t)
	time.Sleep(100 * time.Millisecond) // Give Consul a moment

	// Create test data
	numEntries := 15
	var expectedFullKeys []string
	for i := range numEntries {
		key := fmt.Sprintf("%skey%02d", testPrefix, i)
		entry := &physical.Entry{Key: key, Value: fmt.Appendf(nil, "value%02d", i)}
		err = backend.Put(ctx, entry)
		if err != nil {
			t.Fatalf("Put failed for %s: %v", key, err)
		}
		expectedFullKeys = append(expectedFullKeys, key)
	}

	// Sort expected keys to match List/ListPage output order
	sort.Strings(expectedFullKeys)

	// Convert full keys to relative keys for comparison with ListPage output
	var expectedRelativeKeys []string
	for _, fk := range expectedFullKeys {
		expectedRelativeKeys = append(expectedRelativeKeys, fk[len(testPrefix):])
	}

	t.Run("first page", func(t *testing.T) {
		keys, err := backend.ListPage(ctx, testPrefix, "", 5) // Get first 5 keys
		if err != nil {
			t.Fatalf("ListPage failed: %v", err)
		}
		if len(keys) != 5 {
			t.Fatalf("Expected 5 keys, got %d: %v", len(keys), keys)
		}
		expected := expectedRelativeKeys[0:5]
		for i, key := range keys {
			if key != expected[i] {
				t.Errorf("Mismatch at index %d: expected %s, got %s", i, expected[i], key)
			}
		}
	})

	t.Run("second page", func(t *testing.T) {
		afterKey := expectedRelativeKeys[4] // After the 5th key
		keys, err := backend.ListPage(ctx, testPrefix, afterKey, 5)
		if err != nil {
			t.Fatalf("ListPage failed: %v", err)
		}
		if len(keys) != 5 {
			t.Fatalf("Expected 5 keys, got %d: %v", len(keys), keys)
		}
		expected := expectedRelativeKeys[5:10]
		for i, key := range keys {
			if key != expected[i] {
				t.Errorf("Mismatch at index %d: expected %s, got %s", i, expected[i], key)
			}
		}
	})

	t.Run("last page with fewer items", func(t *testing.T) {
		afterKey := expectedRelativeKeys[9] // After the 10th key
		keys, err := backend.ListPage(ctx, testPrefix, afterKey, 5)
		if err != nil {
			t.Fatalf("ListPage failed: %v", err)
		}
		if len(keys) != 5 { // 15 total keys, 10 already retrieved, so 5 remaining
			t.Fatalf("Expected 5 keys, got %d: %v", len(keys), keys)
		}
		expected := expectedRelativeKeys[10:15]
		for i, key := range keys {
			if key != expected[i] {
				t.Errorf("Mismatch at index %d: expected %s, got %s", i, expected[i], key)
			}
		}
	})

	t.Run("after the last key", func(t *testing.T) {
		afterKey := expectedRelativeKeys[numEntries-1] // After the very last key
		keys, err := backend.ListPage(ctx, testPrefix, afterKey, 5)
		if err != nil {
			t.Fatalf("ListPage failed: %v", err)
		}
		if len(keys) != 0 {
			t.Fatalf("Expected 0 keys, got %d: %v", len(keys), keys)
		}
	})

	t.Run("empty prefix", func(t *testing.T) {
		// This test depends on other tests not polluting the root, or clearing it.
		// For robustness, it's often better to have a dedicated, isolated test setup.
		// For now, we'll just check it returns something without error.
		keys, err := backend.ListPage(ctx, "", "", 1)
		if err != nil {
			t.Fatalf("ListPage with empty prefix failed: %v", err)
		}
		if len(keys) == 0 {
			t.Log("Warning: ListPage with empty prefix returned 0 keys. This might be fine if Consul is empty or specific path is used.")
		}
	})

	t.Run("non-existent prefix", func(t *testing.T) {
		keys, err := backend.ListPage(ctx, "non-existent-prefix/", "", 10)
		if err != nil {
			t.Fatalf("ListPage with non-existent prefix failed: %v", err)
		}
		if len(keys) != 0 {
			t.Fatalf("Expected 0 keys for non-existent prefix, got %d", len(keys))
		}
	})

	// physical.Backend documents the limit as "at most limit entries (negative
	// for unlimited)". inmem treats zero the same way, so both return the lot.
	t.Run("zero limit means unlimited", func(t *testing.T) {
		keys, err := backend.ListPage(ctx, testPrefix, "", 0)
		if err != nil {
			t.Fatalf("ListPage with zero limit failed: %v", err)
		}
		if len(keys) != numEntries {
			t.Fatalf("Expected all %d keys for zero limit, got %d: %v", numEntries, len(keys), keys)
		}
	})

	t.Run("negative limit means unlimited", func(t *testing.T) {
		keys, err := backend.ListPage(ctx, testPrefix, "", -5)
		if err != nil {
			t.Fatalf("ListPage with negative limit failed: %v", err)
		}
		if len(keys) != numEntries {
			t.Fatalf("Expected all %d keys for negative limit, got %d: %v", numEntries, len(keys), keys)
		}
	})

	// The cursor need not still exist: it may have been deleted between pages.
	// The contract asks for the keys that sort after it, so pagination has to
	// resume by comparison rather than restart from the beginning -- a restart
	// makes a paginating caller re-read page one forever.
	t.Run("absent after key resumes by sort order", func(t *testing.T) {
		// Sorts between key06 and key07, and is not itself a key.
		keys, err := backend.ListPage(ctx, testPrefix, "key06x", 3)
		if err != nil {
			t.Fatalf("ListPage with absent after key failed: %v", err)
		}
		expected := expectedRelativeKeys[7:10]
		if len(keys) != len(expected) {
			t.Fatalf("Expected %d keys after an absent cursor, got %d: %v", len(expected), len(keys), keys)
		}
		for i, key := range keys {
			if key != expected[i] {
				t.Errorf("Mismatch at index %d: expected %s, got %s", i, expected[i], key)
			}
		}

		// A cursor sorting beyond every key yields nothing at all.
		keys, err = backend.ListPage(ctx, testPrefix, "non-existent-after-key", 3)
		if err != nil {
			t.Fatalf("ListPage with out-of-range after key failed: %v", err)
		}
		if len(keys) != 0 {
			t.Fatalf("Expected 0 keys past the end of the range, got %d: %v", len(keys), keys)
		}
	})

	// Clean up
	cleanupListPageData(backend.(*ConsulBackend), ctx, testPrefix, t)
}

func TestConsulBackend_BASIC_ConfigValidation(t *testing.T) {
	logger := hclog.NewNullLogger()

	testCases := []struct {
		name      string
		config    map[string]string
		shouldErr bool
	}{
		{
			name: "missing path",
			config: map[string]string{
				"address": "127.0.0.1:8500",
			},
			shouldErr: true,
		},
		{
			name: "invalid address format",
			config: map[string]string{
				"address": "invalid-address",
				"path":    "test/",
			},
			shouldErr: true,
		},
		{
			name: "invalid retry count",
			config: map[string]string{
				"address":     "127.0.0.1:8500",
				"path":        "test/",
				"max_retries": "invalid",
			},
			shouldErr: true,
		},
	}

	// These cases fail during config validation, before any connection attempt,
	// so they run everywhere without a live Consul.
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewConsulBackend(tc.config, logger)
			if tc.shouldErr && err == nil {
				t.Fatalf("Expected error for config %v", tc.config)
			}
			if !tc.shouldErr && err != nil {
				t.Fatalf("Unexpected error for config %v: %v", tc.config, err)
			}
		})
	}

	// A valid minimal config succeeds only against a live Consul, since
	// NewConsulBackend performs a connection test during construction.
	t.Run("valid minimal config", func(t *testing.T) {
		_, err := NewConsulBackend(requireConsul(t, "test/openbao/configvalid/"), logger)
		if err != nil {
			t.Fatalf("Unexpected error for valid minimal config: %v", err)
		}
	})
}

// TestConsulBackend_SDKConformance runs the SDK's backend contract suites, the
// same ones raft and postgresql run and consul did not.
//
// ExerciseBackend covers ListPage directly, including the documented
// "negative for unlimited" sentinel and the behaviour when the after cursor is
// not present among the keys.
func TestConsulBackend_SDKConformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SDK conformance test in short mode")
	}

	logger := hclog.NewNullLogger()

	newBackend := func(t *testing.T, suite string) physical.Backend {
		t.Helper()
		// A prefix per suite and per run keeps the suites, which each assume
		// they start against an empty backend, from seeing each other's keys.
		path := fmt.Sprintf("test/openbao/conformance-%s-%d/", suite, time.Now().UnixNano())
		b, err := NewConsulBackend(requireConsul(t, path), logger)
		if err != nil {
			failOrSkip(t, "Consul not available: %v", err)
		}
		return b
	}

	t.Run("ExerciseBackend", func(t *testing.T) {
		physical.ExerciseBackend(t, newBackend(t, "backend"))
	})

	t.Run("ExerciseBackend_ListPrefix", func(t *testing.T) {
		physical.ExerciseBackend_ListPrefix(t, newBackend(t, "listprefix"))
	})
}
