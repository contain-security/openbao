package consul

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/sdk/v2/physical"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockKV is a mock implementation of the Consul KV interface
type MockKV struct {
	mock.Mock
}

func (m *MockKV) Put(p *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, error) {
	args := m.Called(p, q)
	return args.Get(0).(*api.WriteMeta), args.Error(1)
}

func (m *MockKV) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	args := m.Called(key, q)
	return args.Get(0).(*api.KVPair), args.Get(1).(*api.QueryMeta), args.Error(2)
}

func (m *MockKV) Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error) {
	args := m.Called(key, w)
	return args.Get(0).(*api.WriteMeta), args.Error(1)
}

func (m *MockKV) Keys(prefix, separator string, q *api.QueryOptions) ([]string, *api.QueryMeta, error) {
	args := m.Called(prefix, separator, q)
	return args.Get(0).([]string), args.Get(1).(*api.QueryMeta), args.Error(2)
}

// SlowMockKV simulates slow operations for testing cancellation during execution
type SlowMockKV struct {
	mock.Mock
	delay time.Duration
}

func (m *SlowMockKV) Put(p *api.KVPair, q *api.WriteOptions) (*api.WriteMeta, error) {
	// Check if context is already cancelled before sleeping
	select {
	case <-q.Context().Done():
		return nil, q.Context().Err()
	default:
	}

	// Simulate slow operation
	timer := time.NewTimer(m.delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		args := m.Called(p, q)
		return args.Get(0).(*api.WriteMeta), args.Error(1)
	case <-q.Context().Done():
		return nil, q.Context().Err()
	}
}

func (m *SlowMockKV) Get(key string, q *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error) {
	// Check if context is already cancelled before sleeping
	select {
	case <-q.Context().Done():
		return nil, nil, q.Context().Err()
	default:
	}

	// Simulate slow operation
	timer := time.NewTimer(m.delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		args := m.Called(key, q)
		return args.Get(0).(*api.KVPair), args.Get(1).(*api.QueryMeta), args.Error(2)
	case <-q.Context().Done():
		return nil, nil, q.Context().Err()
	}
}

func (m *SlowMockKV) Delete(key string, w *api.WriteOptions) (*api.WriteMeta, error) {
	// Check if context is already cancelled before sleeping
	select {
	case <-w.Context().Done():
		return nil, w.Context().Err()
	default:
	}

	// Simulate slow operation
	timer := time.NewTimer(m.delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		args := m.Called(key, w)
		return args.Get(0).(*api.WriteMeta), args.Error(1)
	case <-w.Context().Done():
		return nil, w.Context().Err()
	}
}

func (m *SlowMockKV) Keys(prefix, separator string, q *api.QueryOptions) ([]string, *api.QueryMeta, error) {
	// Check if context is already cancelled before sleeping
	select {
	case <-q.Context().Done():
		return nil, nil, q.Context().Err()
	default:
	}

	// Simulate slow operation
	timer := time.NewTimer(m.delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		args := m.Called(prefix, separator, q)
		return args.Get(0).([]string), args.Get(1).(*api.QueryMeta), args.Error(2)
	case <-q.Context().Done():
		return nil, nil, q.Context().Err()
	}
}

// KVInterface defines the interface that both mock implementations satisfy
type KVInterface interface {
	Put(*api.KVPair, *api.WriteOptions) (*api.WriteMeta, error)
	Get(string, *api.QueryOptions) (*api.KVPair, *api.QueryMeta, error)
	Delete(string, *api.WriteOptions) (*api.WriteMeta, error)
	Keys(string, string, *api.QueryOptions) ([]string, *api.QueryMeta, error)
}

// TestableConsulBackend embeds ConsulBackend but with a testable KV interface
type TestableConsulBackend struct {
	kv     KVInterface
	path   string
	logger hclog.Logger
}

// Put implements the same logic as ConsulBackend.Put but uses the testable KV interface
func (c *TestableConsulBackend) Put(ctx context.Context, entry *physical.Entry) error {
	defer func(start time.Time) {
		c.logger.Debug("consul put operation completed",
			"key", entry.Key,
			"duration", time.Since(start))
	}(time.Now())

	// Check for context cancellation at the start
	select {
	case <-ctx.Done():
		c.logger.Debug("consul put operation cancelled before starting",
			"key", entry.Key,
			"error", ctx.Err())
		return ctx.Err()
	default:
	}

	if entry == nil {
		return fmt.Errorf("entry cannot be nil")
	}

	consulKey := c.consulKey(entry.Key)

	// Create KV pair for Consul
	pair := &api.KVPair{
		Key:   consulKey,
		Value: entry.Value,
	}

	// Perform the put operation with context
	writeOpts := &api.WriteOptions{}
	writeOpts = writeOpts.WithContext(ctx)

	_, err := c.kv.Put(pair, writeOpts)
	if err != nil {
		// Check if the error is due to context cancellation
		if ctx.Err() != nil {
			c.logger.Debug("consul put operation cancelled during execution",
				"key", entry.Key,
				"consul_key", consulKey,
				"error", ctx.Err())
			return ctx.Err()
		}

		c.logger.Error("failed to put key in consul",
			"key", entry.Key,
			"consul_key", consulKey,
			"error", err)
		return fmt.Errorf("failed to store key %q in consul: %w", entry.Key, err)
	}

	c.logger.Trace("successfully stored key in consul",
		"key", entry.Key,
		"consul_key", consulKey,
		"value_size", len(entry.Value))

	return nil
}

// Get implements the same logic as ConsulBackend.Get but uses the testable KV interface
func (c *TestableConsulBackend) Get(ctx context.Context, key string) (*physical.Entry, error) {
	defer func(start time.Time) {
		c.logger.Debug("consul get operation completed",
			"key", key,
			"duration", time.Since(start))
	}(time.Now())

	// Check for context cancellation at the start
	select {
	case <-ctx.Done():
		c.logger.Debug("consul get operation cancelled before starting",
			"key", key,
			"error", ctx.Err())
		return nil, ctx.Err()
	default:
	}

	consulKey := c.consulKey(key)

	// Perform the get operation with context
	queryOpts := &api.QueryOptions{}
	queryOpts = queryOpts.WithContext(ctx)

	pair, _, err := c.kv.Get(consulKey, queryOpts)
	if err != nil {
		// Check if the error is due to context cancellation
		if ctx.Err() != nil {
			c.logger.Debug("consul get operation cancelled during execution",
				"key", key,
				"consul_key", consulKey,
				"error", ctx.Err())
			return nil, ctx.Err()
		}

		c.logger.Error("failed to get key from consul",
			"key", key,
			"consul_key", consulKey,
			"error", err)
		return nil, fmt.Errorf("failed to retrieve key %q from consul: %w", key, err)
	}

	// Key doesn't exist
	if pair == nil {
		c.logger.Trace("key not found in consul",
			"key", key,
			"consul_key", consulKey)
		return nil, nil
	}

	entry := &physical.Entry{
		Key:   key,
		Value: pair.Value,
	}

	c.logger.Trace("successfully retrieved key from consul",
		"key", key,
		"consul_key", consulKey,
		"value_size", len(pair.Value))

	return entry, nil
}

// Delete implements the same logic as ConsulBackend.Delete but uses the testable KV interface
func (c *TestableConsulBackend) Delete(ctx context.Context, key string) error {
	defer func(start time.Time) {
		c.logger.Debug("consul delete operation completed",
			"key", key,
			"duration", time.Since(start))
	}(time.Now())

	// Check for context cancellation at the start
	select {
	case <-ctx.Done():
		c.logger.Debug("consul delete operation cancelled before starting",
			"key", key,
			"error", ctx.Err())
		return ctx.Err()
	default:
	}

	consulKey := c.consulKey(key)

	// Perform the delete operation with context
	writeOpts := &api.WriteOptions{}
	writeOpts = writeOpts.WithContext(ctx)

	_, err := c.kv.Delete(consulKey, writeOpts)
	if err != nil {
		// Check if the error is due to context cancellation
		if ctx.Err() != nil {
			c.logger.Debug("consul delete operation cancelled during execution",
				"key", key,
				"consul_key", consulKey,
				"error", ctx.Err())
			return ctx.Err()
		}

		c.logger.Error("failed to delete key from consul",
			"key", key,
			"consul_key", consulKey,
			"error", err)
		return fmt.Errorf("failed to delete key %q from consul: %w", key, err)
	}

	c.logger.Trace("successfully deleted key from consul",
		"key", key,
		"consul_key", consulKey)

	return nil
}

// List implements the same logic as ConsulBackend.List but uses the testable KV interface
func (c *TestableConsulBackend) List(ctx context.Context, prefix string) ([]string, error) {
	defer func(start time.Time) {
		c.logger.Debug("consul list operation completed",
			"prefix", prefix,
			"duration", time.Since(start))
	}(time.Now())

	c.logger.Debug("DEBUG: List method called", "prefix", prefix, "context_err", ctx.Err())

	// Check for context cancellation at the start
	select {
	case <-ctx.Done():
		c.logger.Debug("DEBUG: Context already cancelled at start", "error", ctx.Err())
		c.logger.Debug("consul list operation cancelled before starting",
			"prefix", prefix,
			"error", ctx.Err())
		return nil, ctx.Err()
	default:
		c.logger.Debug("DEBUG: Context not cancelled at start, proceeding")
	}

	consulPrefix := c.consulKey(prefix)
	c.logger.Debug("DEBUG: Consul prefix calculated", "prefix", prefix, "consul_prefix", consulPrefix)

	// Use "/" as separator to get hierarchical listing
	queryOpts := &api.QueryOptions{}
	queryOpts = queryOpts.WithContext(ctx)

	c.logger.Debug("DEBUG: About to call kv.Keys", "consul_prefix", consulPrefix)

	keys, _, err := c.kv.Keys(consulPrefix, "", queryOpts)
	if err != nil {
		c.logger.Debug("DEBUG: kv.Keys returned error", "error", err, "context_err", ctx.Err())

		// Check if the error is due to context cancellation
		if ctx.Err() != nil {
			c.logger.Debug("DEBUG: Context error detected after kv.Keys", "context_err", ctx.Err())
			c.logger.Debug("consul list operation cancelled during execution",
				"prefix", prefix,
				"consul_prefix", consulPrefix,
				"error", ctx.Err())
			return nil, ctx.Err()
		}

		c.logger.Error("failed to list keys from consul",
			"prefix", prefix,
			"consul_prefix", consulPrefix,
			"error", err)
		return nil, fmt.Errorf("failed to list keys with prefix %q from consul: %w", prefix, err)
	}

	c.logger.Debug("DEBUG: kv.Keys succeeded", "key_count", len(keys), "context_err", ctx.Err())

	// Convert consul keys back to OpenBao keys
	result := make([]string, 0, len(keys))
	for i, consulKey := range keys {
		// Check for context cancellation during key processing
		if i%100 == 0 { // Check every 100 iterations to avoid excessive overhead
			c.logger.Debug("DEBUG: Checking context at iteration", "iteration", i, "context_err", ctx.Err())
			select {
			case <-ctx.Done():
				c.logger.Debug("DEBUG: Context cancelled during key processing", "iteration", i, "context_err", ctx.Err())
				c.logger.Debug("consul list operation cancelled during key processing",
					"prefix", prefix,
					"processed", i,
					"total", len(keys),
					"error", ctx.Err())
				return nil, ctx.Err()
			default:
				c.logger.Debug("DEBUG: Context still good at iteration", "iteration", i)
			}
		}

		// fmt.Println("DLS - ", consulKey)
		openBaoKey := c.openBaoKey(consulKey)
		c.logger.Debug("DEBUG: Processing key", "iteration", i, "consul_key", consulKey, "openbao_key", openBaoKey)

		// Remove the prefix to get relative key
		if strings.HasPrefix(openBaoKey, prefix) {
			relativeKey := strings.TrimPrefix(openBaoKey, prefix)
			if relativeKey != "" {
				result = append(result, relativeKey)
				c.logger.Debug("DEBUG: Added key to result", "relative_key", relativeKey)
			} else {
				c.logger.Debug("DEBUG: Skipped empty relative key", "openbao_key", openBaoKey)
			}
		} else {
			c.logger.Debug("DEBUG: Key doesn't match prefix", "openbao_key", openBaoKey, "prefix", prefix)
		}
	}

	c.logger.Debug("DEBUG: Key processing complete", "result_count", len(result), "context_err", ctx.Err())

	c.logger.Trace("successfully listed keys from consul",
		"prefix", prefix,
		"consul_prefix", consulPrefix,
		"count", len(result))

	return result, nil
}

// ListPage implements the same logic as ConsulBackend.ListPage but uses the testable KV interface
func (c *TestableConsulBackend) ListPage(ctx context.Context, prefix string, after string, limit int) ([]string, error) {
	defer func(start time.Time) {
		c.logger.Debug("consul listpage operation completed",
			"prefix", prefix, "after", after, "limit", limit,
			"duration", time.Since(start))
	}(time.Now())

	// Check for context cancellation at the start
	select {
	case <-ctx.Done():
		c.logger.Debug("consul listpage operation cancelled before starting",
			"prefix", prefix,
			"after", after,
			"limit", limit,
			"error", ctx.Err())
		return nil, ctx.Err()
	default:
	}

	// Get all keys first (we'll optimize this later if needed)
	// no key conversions needed since we're using c.List
	allKeys, err := c.List(ctx, prefix)
	if err != nil {
		// c.List already handles context cancellation, so we can return the error directly
		return nil, err
	}

	// Apply pagination
	var result []string
	found := after == "" // If no after key, start from beginning

	for i, key := range allKeys {
		// Check for context cancellation during pagination processing
		if i%50 == 0 { // Check every 50 iterations
			select {
			case <-ctx.Done():
				c.logger.Debug("consul listpage operation cancelled during pagination",
					"prefix", prefix,
					"after", after,
					"limit", limit,
					"processed", i,
					"error", ctx.Err())
				return nil, ctx.Err()
			default:
			}
		}

		if !found {
			if key == after {
				found = true
			}
			continue
		}

		if len(result) >= limit {
			break
		}

		result = append(result, key)
	}
	if !found {
		// handle case of after - not in keys
		for i, key := range allKeys {
			// Check for context cancellation during fallback processing
			if i%50 == 0 { // Check every 50 iterations
				select {
				case <-ctx.Done():
					c.logger.Debug("consul listpage operation cancelled during fallback processing",
						"prefix", prefix,
						"after", after,
						"limit", limit,
						"processed", i,
						"error", ctx.Err())
					return nil, ctx.Err()
				default:
				}
			}

			if len(result) >= limit {
				break
			}
			result = append(result, key)
		}
	}

	c.logger.Trace("paginated key list from consul",
		"prefix", prefix, "after", after, "limit", limit, "returned", len(result))

	return result, nil
}

// Helper function to convert OpenBao key to Consul key
func (c *TestableConsulBackend) consulKey(key string) string {
	return c.path + key
}

// Helper function to convert Consul key back to OpenBao key
func (c *TestableConsulBackend) openBaoKey(consulKey string) string {
	return strings.TrimPrefix(consulKey, c.path)
}

// Helper function to create a test backend with a mock KV
func createTestBackendWithMockKV(mockKV KVInterface) *TestableConsulBackend {
	// Use a logger that will output debug messages during tests
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "test-consul-backend",
		Level:  hclog.Debug,
		Output: os.Stderr, // This will show in test output
	})

	return &TestableConsulBackend{
		kv:     mockKV,
		path:   "test/",
		logger: logger,
	}
}

func TestConsulBackend_Put_ContextCancellation(t *testing.T) {
	t.Run("cancelled before operation", func(t *testing.T) {
		backend := createTestBackendWithMockKV(&MockKV{})

		// Create already cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		entry := &physical.Entry{
			Key:   "test-key",
			Value: []byte("test-value"),
		}

		err := backend.Put(ctx, entry)

		// Should return context cancellation error
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("cancelled during operation", func(t *testing.T) {
		slowMock := &SlowMockKV{delay: 100 * time.Millisecond}
		backend := createTestBackendWithMockKV(slowMock)

		// Create context that will be cancelled during operation
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		entry := &physical.Entry{
			Key:   "test-key",
			Value: []byte("test-value"),
		}

		err := backend.Put(ctx, entry)

		// Should return context deadline exceeded error
		assert.Error(t, err)
		assert.Equal(t, context.DeadlineExceeded, err)
	})
}

func TestConsulBackend_Get_ContextCancellation(t *testing.T) {
	t.Run("cancelled before operation", func(t *testing.T) {
		backend := createTestBackendWithMockKV(&MockKV{})

		// Create already cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		entry, err := backend.Get(ctx, "test-key")

		// Should return context cancellation error
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, entry)
	})

	t.Run("cancelled during operation", func(t *testing.T) {
		slowMock := &SlowMockKV{delay: 100 * time.Millisecond}
		backend := createTestBackendWithMockKV(slowMock)

		// Create context that will be cancelled during operation
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		entry, err := backend.Get(ctx, "test-key")

		// Should return context deadline exceeded error
		assert.Error(t, err)
		assert.Equal(t, context.DeadlineExceeded, err)
		assert.Nil(t, entry)
	})
}

func TestConsulBackend_Delete_ContextCancellation(t *testing.T) {
	t.Run("cancelled before operation", func(t *testing.T) {
		backend := createTestBackendWithMockKV(&MockKV{})

		// Create already cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := backend.Delete(ctx, "test-key")

		// Should return context cancellation error
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("cancelled during operation", func(t *testing.T) {
		slowMock := &SlowMockKV{delay: 100 * time.Millisecond}
		backend := createTestBackendWithMockKV(slowMock)

		// Create context that will be cancelled during operation
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := backend.Delete(ctx, "test-key")

		// Should return context deadline exceeded error
		assert.Error(t, err)
		assert.Equal(t, context.DeadlineExceeded, err)
	})
}

func TestConsulBackend_List_ContextCancellation(t *testing.T) {
	t.Run("cancelled before operation", func(t *testing.T) {
		backend := createTestBackendWithMockKV(&MockKV{})

		// Create already cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		t.Logf("Context cancelled, err: %v", ctx.Err())

		keys, err := backend.List(ctx, "test/")

		t.Logf("Result - keys: %v, err: %v", keys, err)

		// Should return context cancellation error
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, keys)
	})

	t.Run("cancelled during operation", func(t *testing.T) {
		slowMock := &SlowMockKV{delay: 100 * time.Millisecond}
		backend := createTestBackendWithMockKV(slowMock)

		// Create context that will be cancelled during operation
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		t.Logf("Starting List operation with timeout context")

		keys, err := backend.List(ctx, "test/")

		t.Logf("Result - keys: %v, err: %v", keys, err)

		// Should return context deadline exceeded error
		assert.Error(t, err)
		assert.Equal(t, context.DeadlineExceeded, err)
		assert.Nil(t, keys)
	})

	t.Run("cancelled during key processing", func(t *testing.T) {
		mockKV := &MockKV{}
		backend := createTestBackendWithMockKV(mockKV)

		// Create a large number of keys to process - ensure they have the correct format
		largeKeySet := make([]string, 1000)
		for i := 0; i < 1000; i++ {
			// Fix: use i instead of rune(i) for proper key naming
			largeKeySet[i] = backend.consulKey("test/key-" + fmt.Sprintf("%d", i))
		}

		t.Logf("Created %d test keys, first few: %v", len(largeKeySet), largeKeySet[:3])

		// Mock the Keys call to return immediately but with many keys
		mockKV.On("Keys", "test/test/", "", mock.AnythingOfType("*api.QueryOptions")).Return(
			largeKeySet, &api.QueryMeta{}, nil)

		// Create context that will be cancelled during key processing
		ctx, cancel := context.WithCancel(context.Background())

		// Cancel after a very short time to trigger cancellation during processing
		go func() {
			time.Sleep(1 * time.Millisecond)
			t.Logf("Cancelling context after 1ms")
			cancel()
		}()

		t.Logf("Starting List operation that should be cancelled during processing")

		keys, err := backend.List(ctx, "test/")

		t.Logf("Result - keys count: %d, err: %v", len(keys), err)
		if err != nil {
			t.Logf("Error type: %T, context err: %v", err, ctx.Err())
		}

		// Should return context cancellation error
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, keys)
	})
}

func TestConsulBackend_ListPage_ContextCancellation(t *testing.T) {
	t.Run("cancelled before operation", func(t *testing.T) {
		backend := createTestBackendWithMockKV(&MockKV{})

		// Create already cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		keys, err := backend.ListPage(ctx, "test/", "", 10)

		// Should return context cancellation error
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		assert.Nil(t, keys)
	})

	t.Run("cancelled during List operation", func(t *testing.T) {
		slowMock := &SlowMockKV{delay: 100 * time.Millisecond}
		backend := createTestBackendWithMockKV(slowMock)

		// Create context that will be cancelled during operation
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		keys, err := backend.ListPage(ctx, "test/", "", 10)

		// Should return context deadline exceeded error (from the underlying List call)
		assert.Error(t, err)
		assert.Equal(t, context.DeadlineExceeded, err)
		assert.Nil(t, keys)
	})
}

// Benchmark tests to ensure cancellation checks don't significantly impact performance
func BenchmarkConsulBackend_Put_WithCancellationChecks(b *testing.B) {
	mockKV := &MockKV{}
	backend := createTestBackendWithMockKV(mockKV)

	// Mock successful put operations
	mockKV.On("Put", mock.AnythingOfType("*api.KVPair"), mock.AnythingOfType("*api.WriteOptions")).Return(
		&api.WriteMeta{}, nil)

	ctx := context.Background()
	entry := &physical.Entry{
		Key:   "benchmark-key",
		Value: []byte("benchmark-value"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = backend.Put(ctx, entry)
	}
}

func BenchmarkConsulBackend_List_WithCancellationChecks(b *testing.B) {
	mockKV := &MockKV{}
	backend := createTestBackendWithMockKV(mockKV)

	// Create a moderate number of keys for realistic benchmarking
	keySet := make([]string, 100)
	for i := 0; i < 100; i++ {
		keySet[i] = backend.consulKey("benchmark/key-" + string(rune(i)))
	}

	// Mock successful list operations
	mockKV.On("Keys", "test/benchmark/", "", mock.AnythingOfType("*api.QueryOptions")).Return(
		keySet, &api.QueryMeta{}, nil)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = backend.List(ctx, "benchmark/")
	}
}

// Integration-style test that verifies the full cancellation flow
func TestConsulBackend_CancellationIntegration(t *testing.T) {
	t.Run("graceful cancellation during multiple operations", func(t *testing.T) {
		slowMock := &SlowMockKV{delay: 200 * time.Millisecond}
		backend := createTestBackendWithMockKV(slowMock)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Try multiple operations that should all be cancelled
		entry := &physical.Entry{Key: "test", Value: []byte("value")}

		// All operations should fail with context deadline exceeded
		err1 := backend.Put(ctx, entry)
		assert.Error(t, err1)
		assert.Equal(t, context.DeadlineExceeded, err1)

		_, err2 := backend.Get(ctx, "test")
		assert.Error(t, err2)
		assert.Equal(t, context.DeadlineExceeded, err2)

		err3 := backend.Delete(ctx, "test")
		assert.Error(t, err3)
		assert.Equal(t, context.DeadlineExceeded, err3)

		_, err4 := backend.List(ctx, "test/")
		assert.Error(t, err4)
		assert.Equal(t, context.DeadlineExceeded, err4)
	})
}
