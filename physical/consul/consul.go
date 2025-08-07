package consul

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-hclog"
	"github.com/openbao/openbao/sdk/v2/physical"
)

// compile-time interface checks to ensure compile knows available interfaces
var (
	_ physical.Backend   = (*ConsulBackend)(nil)
	_ physical.HABackend = (*ConsulBackend)(nil)
	_ physical.Lock      = (*ConsulLock)(nil)
)

type ServiceStatus struct {
	Initialized bool
	Sealed      bool
	Active      bool
	Version     string
}

// ConsulBackend implements physical.Backend using Consul KV store
type ConsulBackend struct {
	client      *api.Client
	kv          *api.KV
	path        string
	logger      hclog.Logger
	retryConfig RetryConfig
	// Add TLS fields
	tlsConfig  *tls.Config
	tlsEnabled bool
	aclEnabled bool
	token      string

	// HA-specific fields
	haEnabled bool
	// advertiseAddr string
	sessionTTL time.Duration
	lockDelay  time.Duration
}

// ConsulLock implements the physical.Lock interface
type ConsulLock struct {
	backend    *ConsulBackend
	key        string
	value      string
	logger     hclog.Logger
	sessionTTL time.Duration
	lockDelay  time.Duration

	// Internal state
	session  string
	stopCh   chan struct{}
	doneCh   chan struct{}
	mu       sync.Mutex // Protect internal state
	unlocked bool       // Track if we've been unlocked
}

// RetryConfig holds retry configuration parameters
type RetryConfig struct {
	MaxRetries      int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	MaxElapsedTime  time.Duration
	Multiplier      float64
}

// DefaultRetryConfig returns sensible default retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:      3,
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     2 * time.Second,
		MaxElapsedTime:  10 * time.Second,
		Multiplier:      2.0,
	}
}

// validateConfig validates the Consul backend configuration
func validateConfig(conf map[string]string, logger hclog.Logger) error {
	// Validate required fields
	if path := conf["path"]; path == "" {
		return fmt.Errorf("'path' configuration parameter is required")
	}

	// Validate address format
	if address := conf["address"]; address != "" {
		if !strings.Contains(address, ":") {
			return fmt.Errorf("invalid address format %q: must include port (e.g., '127.0.0.1:8500')", address)
		}
	}

	// Validate TLS configuration
	if tlsEnabled := conf["tls_enabled"]; tlsEnabled == "true" || tlsEnabled == "1" {
		// If TLS is enabled, validate certificate files exist
		if caCert := conf["tls_ca_cert"]; caCert != "" {
			if _, err := os.Stat(caCert); os.IsNotExist(err) {
				return fmt.Errorf("TLS CA certificate file does not exist: %s", caCert)
			}
		}

		if clientCert := conf["tls_client_cert"]; clientCert != "" {
			if _, err := os.Stat(clientCert); os.IsNotExist(err) {
				return fmt.Errorf("TLS client certificate file does not exist: %s", clientCert)
			}

			clientKey := conf["tls_client_key"]
			if clientKey == "" {
				return fmt.Errorf("tls_client_key must be provided when tls_client_cert is specified")
			}
			if _, err := os.Stat(clientKey); os.IsNotExist(err) {
				return fmt.Errorf("TLS client key file does not exist: %s", clientKey)
			}
		}
	}

	// Validate retry configuration
	if retriesStr := conf["max_retries"]; retriesStr != "" {
		if retries, err := strconv.Atoi(retriesStr); err != nil {
			return fmt.Errorf("invalid max_retries value %q: must be a number", retriesStr)
		} else if retries < 0 {
			return fmt.Errorf("max_retries cannot be negative: %d", retries)
		}
	}

	if delayStr := conf["retry_delay"]; delayStr != "" {
		if _, err := time.ParseDuration(delayStr); err != nil {
			return fmt.Errorf("invalid retry_delay value %q: %w", delayStr, err)
		}
	}

	// Validate token file exists if specified
	if tokenFile := conf["token_file"]; tokenFile != "" {
		if _, err := os.Stat(tokenFile); os.IsNotExist(err) {
			return fmt.Errorf("token file does not exist: %s", tokenFile)
		}
	}

	// Warn about insecure configurations
	if conf["tls_skip_verify"] == "true" {
		logger.Warn("TLS certificate verification is disabled - this is insecure for production use")
	}

	return nil
}

// HealthCheck verifies connectivity to Consul
func (c *ConsulBackend) HealthCheck(ctx context.Context) error {
	// Test basic connectivity by getting cluster leader
	leader, err := c.client.Status().Leader()
	if err != nil {
		return fmt.Errorf("failed to contact consul cluster: %w", err)
	}

	if leader == "" {
		return fmt.Errorf("consul cluster has no leader")
	}

	c.logger.Debug("consul health check passed", "leader", leader)

	// Test KV permissions by attempting to read from our path
	testKey := c.consulKey("_health_check_test")

	// Try to read (this tests both connectivity and permissions)
	queryOpts := (&api.QueryOptions{}).WithContext(ctx)
	_, _, err = c.kv.Get(testKey, queryOpts)
	if err != nil {
		return fmt.Errorf("failed to test KV access: %w", err)
	}

	c.logger.Debug("consul KV access check passed")
	return nil
}

// NewConsulBackend creates a new Consul storage backend with ACL support
func NewConsulBackend(conf map[string]string, logger hclog.Logger) (physical.Backend, error) {
	// Validate configuration first
	if err := validateConfig(conf, logger); err != nil {
		return nil, fmt.Errorf("consul backend configuration error: %w", err)
	}

	// Parse configuration
	address := conf["address"]
	if address == "" {
		address = "127.0.0.1:8500"
	}

	scheme := conf["scheme"]
	if scheme == "" {
		scheme = "http"
	}

	path := conf["path"]
	if path == "" {
		path = "openbao/"
	}
	// Ensure path ends with /
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	// ACL Configuration
	token := conf["token"]
	aclEnabled := false

	// Check if ACLs are explicitly enabled or if a token is provided
	if aclEnabledStr := conf["acl_enabled"]; aclEnabledStr == "true" || aclEnabledStr == "1" {
		aclEnabled = true
	} else if token != "" {
		// If token is provided, assume ACLs are enabled
		aclEnabled = true
	}

	// If ACLs are enabled but no token provided, that's an error
	if aclEnabled && token == "" {
		return nil, fmt.Errorf("ACL token is required when ACLs are enabled (set 'token' in configuration)")
	}

	// Parse retry configuration
	retryConfig := DefaultRetryConfig()

	if maxRetriesStr := conf["max_retries"]; maxRetriesStr != "" {
		if maxRetries, err := strconv.Atoi(maxRetriesStr); err == nil && maxRetries >= 0 {
			retryConfig.MaxRetries = maxRetries
		}
	}

	if initialIntervalStr := conf["retry_initial_interval"]; initialIntervalStr != "" {
		if initialInterval, err := time.ParseDuration(initialIntervalStr); err == nil {
			retryConfig.InitialInterval = initialInterval
		}
	}

	if maxIntervalStr := conf["retry_max_interval"]; maxIntervalStr != "" {
		if maxInterval, err := time.ParseDuration(maxIntervalStr); err == nil {
			retryConfig.MaxInterval = maxInterval
		}
	}

	if maxElapsedTimeStr := conf["retry_max_elapsed_time"]; maxElapsedTimeStr != "" {
		if maxElapsedTime, err := time.ParseDuration(maxElapsedTimeStr); err == nil {
			retryConfig.MaxElapsedTime = maxElapsedTime
		}
	}

	if multiplierStr := conf["retry_multiplier"]; multiplierStr != "" {
		if multiplier, err := strconv.ParseFloat(multiplierStr, 64); err == nil && multiplier > 1.0 {
			retryConfig.Multiplier = multiplier
		}
	}

	// Create Consul client
	config := api.DefaultConfig()
	config.Address = address
	config.Scheme = scheme
	if token != "" {
		config.Token = token
	}

	// Configure TLS
	tlsEnabled := false
	var tlsConfig *tls.Config

	if tlsStr := conf["tls_enabled"]; tlsStr == "true" || tlsStr == "1" {
		tlsEnabled = true
		// Create TLS config
		tlsConfig = &tls.Config{}

		// TLS Skip Verify (for development/self-signed certs)
		if skipVerify := conf["tls_skip_verify"]; skipVerify == "true" || skipVerify == "1" {
			tlsConfig.InsecureSkipVerify = true
			logger.Warn("TLS certificate verification disabled")
		}

		// CA Certificate
		if caCert := conf["tls_ca_cert"]; caCert != "" {
			caCertPool := x509.NewCertPool()
			caCertData, err := os.ReadFile(caCert)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA certificate file %q: %w", caCert, err)
			}
			if !caCertPool.AppendCertsFromPEM(caCertData) {
				return nil, fmt.Errorf("failed to parse CA certificate from %q", caCert)
			}
			logger.Info("CA certificate loaded successfully", "file", caCert)
			tlsConfig.RootCAs = caCertPool
		}

		// Client Certificate Authentication
		if clientCert := conf["tls_client_cert"]; clientCert != "" {
			clientKey := conf["tls_client_key"]
			if clientKey == "" {
				return nil, fmt.Errorf("tls_client_key must be provided when tls_client_cert is specified")
			}

			cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
			if err != nil {
				return nil, fmt.Errorf("failed to load client certificate: %w", err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
			logger.Info("Client certificate loaded successfully", "cert", clientCert, "key", clientKey)
		}

		// TLS Server Name (for SNI)
		if serverName := conf["tls_server_name"]; serverName != "" {
			tlsConfig.ServerName = serverName
		}

		// Override scheme to https when TLS is enabled
		config.Scheme = "https"

		// Ensure HttpClient and Transport are properly initialized
		if config.HttpClient == nil {
			config.HttpClient = &http.Client{}
		}

		// Create a new transport or clone the existing one
		var transport *http.Transport
		if config.HttpClient.Transport != nil {
			// If there's an existing transport, try to cast it to *http.Transport
			if existingTransport, ok := config.HttpClient.Transport.(*http.Transport); ok {
				// Clone the existing transport
				transport = existingTransport.Clone()
			} else {
				// Create a new transport if we can't use the existing one
				transport = &http.Transport{}
			}
		} else {
			transport = &http.Transport{}
		}

		// Apply TLS config to the transport
		transport.TLSClientConfig = tlsConfig
		config.HttpClient.Transport = transport

		logger.Info("TLS enabled for Consul connection", "scheme", config.Scheme, "address", config.Address)
	}

	client, err := api.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Consul client: %w", err)
	}

	// Configure HA
	haEnabled := false
	if haStr := conf["ha_enabled"]; haStr == "true" || haStr == "1" {
		haEnabled = true
	}
	//advertiseAddr := conf["advertise_addr"]
	//if haEnabled && advertiseAddr == "" {
	//	fmt.Printf("DEBUG: HA enabled but advertise_addr not provided. conf=%v\n", conf)
	//	//logger.Debug("HA enabled but advertise_addr not provided", "conf", conf)
	//	return nil, fmt.Errorf("advertise_addr is required when HA is enabled")
	//}

	// Parse HA timing configuration with validation
	sessionTTL := 15 * time.Second
	if ttlStr := conf["session_ttl"]; ttlStr != "" {
		if parsed, err := time.ParseDuration(ttlStr); err == nil {
			sessionTTL = validateSessionTTL(parsed)
			if sessionTTL != parsed {
				logger.Warn("adjusted session_ttl to meet Consul requirements",
					"requested", parsed, "adjusted", sessionTTL)
			}
		}
	} else {
		sessionTTL = validateSessionTTL(sessionTTL)
	}

	lockDelay := 15 * time.Second
	if delayStr := conf["lock_delay"]; delayStr != "" {
		if parsed, err := time.ParseDuration(delayStr); err == nil {
			lockDelay = parsed
		}
	}

	// Create backend instance
	backend := &ConsulBackend{
		client:      client,
		kv:          client.KV(),
		path:        path,
		logger:      logger,
		retryConfig: retryConfig,
		tlsConfig:   tlsConfig,
		tlsEnabled:  tlsEnabled,
		aclEnabled:  aclEnabled,
		token:       token,
		haEnabled:   haEnabled,
		// advertiseAddr: advertiseAddr,
		sessionTTL: sessionTTL,
		lockDelay:  lockDelay,
	}

	// Test basic connection first
	err = backend.withRetry(context.Background(), "connection_test", func() error {
		_, err := client.Status().Leader()
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Consul: %w", err)
	}

	// Test ACL permissions if ACLs are enabled
	if aclEnabled {
		if err := backend.validateACLPermissions(context.Background()); err != nil {
			return nil, fmt.Errorf("ACL validation failed: %w", err)
		}
		logger.Info("ACL permissions validated successfully", "path", path, "token_type", backend.getTokenType())
	}

	logger.Info("Consul backend initialized successfully",
		"address", address,
		"scheme", config.Scheme,
		"tls_enabled", tlsEnabled,
		"acl_enabled", aclEnabled,
		"path", path)
	return backend, nil
}

// validateACLPermissions tests that the current token has the required permissions
func (c *ConsulBackend) validateACLPermissions(ctx context.Context) error {
	testKey := c.path + ".acl_test"
	testValue := []byte("acl_test_value")

	// Test write permission
	err := c.withRetry(ctx, "acl_write_test", func() error {
		_, err := c.kv.Put(&api.KVPair{
			Key:   testKey,
			Value: testValue,
		}, nil)
		return err
	})
	if err != nil {
		if isACLError(err) {
			return fmt.Errorf("insufficient ACL permissions for write operations on path %q: %w", c.path, err)
		}
		return fmt.Errorf("failed to test write permissions: %w", err)
	}

	// Test read permission
	err = c.withRetry(ctx, "acl_read_test", func() error {
		pair, _, err := c.kv.Get(testKey, nil)
		if err != nil {
			return err
		}
		if pair == nil {
			return fmt.Errorf("test key not found after write")
		}
		return nil
	})
	if err != nil {
		if isACLError(err) {
			return fmt.Errorf("insufficient ACL permissions for read operations on path %q: %w", c.path, err)
		}
		return fmt.Errorf("failed to test read permissions: %w", err)
	}

	// Test delete permission (cleanup)
	err = c.withRetry(ctx, "acl_delete_test", func() error {
		_, err := c.kv.Delete(testKey, nil)
		return err
	})
	if err != nil {
		if isACLError(err) {
			c.logger.Warn("insufficient ACL permissions for delete operations, but continuing", "path", c.path, "error", err)
		} else {
			c.logger.Warn("failed to cleanup test key", "key", testKey, "error", err)
		}
	}

	// Test list permission
	err = c.withRetry(ctx, "acl_list_test", func() error {
		_, _, err := c.kv.List(c.path, nil)
		return err
	})
	if err != nil {
		if isACLError(err) {
			return fmt.Errorf("insufficient ACL permissions for list operations on path %q: %w", c.path, err)
		}
		return fmt.Errorf("failed to test list permissions: %w", err)
	}

	return nil
}

// getTokenType attempts to determine the token type for logging purposes
func (c *ConsulBackend) getTokenType() string {
	if c.token == "" {
		return "none"
	}

	// Try to get token info (this requires the token to read itself)
	token, _, err := c.client.ACL().TokenReadSelf(nil)
	if err != nil {
		return "unknown"
	}

	if token == nil {
		return "invalid"
	}

	// Check if it's a management token by looking at policies
	for _, policy := range token.Policies {
		if policy.Name == "global-management" {
			return "management"
		}
	}

	return "client"
}

// isACLError checks if an error is related to ACL permissions
func isACLError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "permission denied") ||
		strings.Contains(errStr, "acl not found") ||
		strings.Contains(errStr, "token not found") ||
		strings.Contains(errStr, "forbidden") ||
		strings.Contains(errStr, "unauthorized")
}

// withRetry executes the given operation with exponential backoff retry logic
func (c *ConsulBackend) withRetry(ctx context.Context, operation string, fn func() error) error {
	// Create exponential backoff with context
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = c.retryConfig.InitialInterval
	b.MaxInterval = c.retryConfig.MaxInterval
	b.MaxElapsedTime = c.retryConfig.MaxElapsedTime
	b.Multiplier = c.retryConfig.Multiplier
	b.RandomizationFactor = 0.1 // Add some jitter

	// Wrap with context and max retries
	backoffStrategy := backoff.WithContext(
		backoff.WithMaxRetries(b, uint64(c.retryConfig.MaxRetries)),
		ctx,
	)

	// var lastErr error
	retryCount := 0

	retryableOperation := func() error {
		err := fn()
		if err != nil {
			retryCount++
			// lastErr = err

			// Log retry attempt
			c.logger.Debug("consul operation failed, retrying",
				"operation", operation,
				"attempt", retryCount,
				"max_retries", c.retryConfig.MaxRetries,
				"error", err)

			// Check if error is retryable
			if !c.isRetryableError(err) {
				c.logger.Debug("consul operation failed with non-retryable error",
					"operation", operation,
					"error", err)
				return backoff.Permanent(err)
			}

			return err
		}

		// Log successful retry if we had previous failures
		if retryCount > 0 {
			c.logger.Info("consul operation succeeded after retries",
				"operation", operation,
				"attempts", retryCount+1)
		}

		return nil
	}

	err := backoff.Retry(retryableOperation, backoffStrategy)
	if err != nil {
		c.logger.Error("consul operation failed after all retries",
			"operation", operation,
			"attempts", retryCount,
			"max_retries", c.retryConfig.MaxRetries,
			"final_error", err)
		return err
	}

	return nil
}

// isRetryableError determines if an error should trigger a retry
func (c *ConsulBackend) isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// Network-related errors that are typically transient
	retryablePatterns := []string{
		"connection refused",
		"connection reset",
		"timeout",
		"temporary failure",
		"network is unreachable",
		"no route to host",
		"connection timed out",
		"i/o timeout",
		"service unavailable",
		"bad gateway",
		"gateway timeout",
		"too many requests",
		"rate limit",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	// Don't retry on context cancellation or authentication errors
	nonRetryablePatterns := []string{
		"context canceled",
		"context deadline exceeded",
		"permission denied",
		"unauthorized",
		"forbidden",
		"invalid token",
		"acl not found",
	}

	for _, pattern := range nonRetryablePatterns {
		if strings.Contains(errStr, pattern) {
			return false
		}
	}

	// Default to retryable for unknown errors
	return true
}

// Put stores a key-value pair in Consul
func (c *ConsulBackend) Put(ctx context.Context, entry *physical.Entry) error {
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

	return c.withRetry(ctx, "put", func() error {
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
					"consul_key set", (consulKey != nil),
					"error", ctx.Err())
				return ctx.Err()
			}

			return fmt.Errorf("failed to store key %q in consul: %w", entry.Key, err)
		}

		c.logger.Trace("successfully stored key in consul",
			"key", entry.Key,
			"consul_key set", (consulKey != nil),,
			"value_size", len(entry.Value))

		return nil
	})
}

// Get retrieves a value by key from Consul
func (c *ConsulBackend) Get(ctx context.Context, key string) (*physical.Entry, error) {
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
	var result *physical.Entry

	err := c.withRetry(ctx, "get", func() error {
		// Perform the get operation with context
		queryOpts := &api.QueryOptions{}
		queryOpts = queryOpts.WithContext(ctx)

		pair, _, err := c.kv.Get(consulKey, queryOpts)
		if err != nil {
			// Check if the error is due to context cancellation
			if ctx.Err() != nil {
				c.logger.Debug("consul get operation cancelled during execution",
					"key", key,
					"consul_key set", (consulKey != nil),
					"error", ctx.Err())
				return ctx.Err()
			}

			return fmt.Errorf("failed to retrieve key %q from consul: %w", key, err)
		}

		// Key doesn't exist
		if pair == nil {
			c.logger.Trace("key not found in consul",
				"key", key,
				"consul_key set", (consulKey != nil))
			result = nil
			return nil
		}

		result = &physical.Entry{
			Key:   key,
			Value: pair.Value,
		}

		c.logger.Trace("successfully retrieved key from consul",
			"key", key,
			"consul_key set", (consulKey != nil),,
			"value_size", len(pair.Value))

		return nil
	})

	return result, err
}

// Delete removes a key-value pair from Consul
func (c *ConsulBackend) Delete(ctx context.Context, key string) error {
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

	return c.withRetry(ctx, "delete", func() error {
		// Perform the delete operation with context
		writeOpts := &api.WriteOptions{}
		writeOpts = writeOpts.WithContext(ctx)

		_, err := c.kv.Delete(consulKey, writeOpts)
		if err != nil {
			// Check if the error is due to context cancellation
			if ctx.Err() != nil {
				c.logger.Debug("consul delete operation cancelled during execution",
					"key", key,
					"consul_key set", (consulKey != nil),,
					"error", ctx.Err())
				return ctx.Err()
			}

			return fmt.Errorf("failed to delete key %q from consul: %w", key, err)
		}

		c.logger.Trace("successfully deleted key from consul",
			"key", key,
			"consul_key set", (consulKey != nil),)

		return nil
	})
}

// List returns all keys with the given prefix
func (c *ConsulBackend) List(ctx context.Context, prefix string) ([]string, error) {
	defer func(start time.Time) {
		c.logger.Debug("consul list operation completed",
			"prefix", prefix,
			"duration", time.Since(start))
	}(time.Now())

	// Check for context cancellation at the start
	select {
	case <-ctx.Done():
		c.logger.Debug("consul list operation cancelled before starting",
			"prefix", prefix,
			"error", ctx.Err())
		return nil, ctx.Err()
	default:
	}

	consulPrefix := c.consulKey(prefix)
	var result []string

	err := c.withRetry(ctx, "list", func() error {
		// Use "/" as separator to get hierarchical listing
		queryOpts := &api.QueryOptions{}
		queryOpts = queryOpts.WithContext(ctx)

		keys, _, err := c.kv.Keys(consulPrefix, "", queryOpts)
		if err != nil {
			// Check if the error is due to context cancellation
			if ctx.Err() != nil {
				c.logger.Debug("consul list operation cancelled during execution",
					"prefix", prefix,
					"consul_prefix", consulPrefix,
					"error", ctx.Err())
				return ctx.Err()
			}

			return fmt.Errorf("failed to list keys with prefix %q from consul: %w", prefix, err)
		}

		// Convert consul keys back to OpenBao keys
		result = make([]string, 0, len(keys))
		for i, consulKey := range keys {
			// Check for context cancellation during key processing
			if i%100 == 0 { // Check every 100 iterations to avoid excessive overhead
				select {
				case <-ctx.Done():
					c.logger.Debug("consul list operation cancelled during key processing",
						"prefix", prefix,
						"processed", i,
						"total", len(keys),
						"error", ctx.Err())
					return ctx.Err()
				default:
				}
			}
			openBaoKey := c.openBaoKey(consulKey)
			// Remove the prefix to get relative key
			if strings.HasPrefix(openBaoKey, prefix) {
				relativeKey := strings.TrimPrefix(openBaoKey, prefix)
				if relativeKey != "" {
					result = append(result, relativeKey)
				}
			}
		}

		c.logger.Trace("successfully listed keys from consul",
			"prefix", prefix,
			"consul_prefix", consulPrefix,
			"count", len(result))

		return nil
	})

	return result, err
}

// ListPage returns paginated keys with the given prefix
func (c *ConsulBackend) ListPage(ctx context.Context, prefix string, after string, limit int) ([]string, error) {
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
		// c.List already handles context cancellation and retries, so we can return the error directly
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
func (c *ConsulBackend) consulKey(key string) string {
	return c.path + key
}

// Helper function to convert Consul key back to OpenBao key
func (c *ConsulBackend) openBaoKey(consulKey string) string {
	return strings.TrimPrefix(consulKey, c.path)
}

// GetTLSConfig returns the TLS configuration for testing purposes
func (c *ConsulBackend) GetTLSConfig() *tls.Config {
	return c.tlsConfig
}

// IsTLSEnabled returns whether TLS is enabled for testing purposes
func (c *ConsulBackend) IsTLSEnabled() bool {
	return c.tlsEnabled
}

// HAEnabled returns whether HA is enabled
func (c *ConsulBackend) HAEnabled() bool {
	return c.haEnabled
}

// HA Operations
// Add TTL validation to NewConsulBackend function
func validateSessionTTL(ttl time.Duration) time.Duration {
	const (
		minTTL     = 10 * time.Second // Consul minimum
		maxTTL     = 24 * time.Hour   // Consul maximum
		defaultTTL = 15 * time.Second
	)

	if ttl < minTTL {
		return minTTL
	}
	if ttl > maxTTL {
		return maxTTL
	}
	return ttl
}

// updated with session creation in Lock method
func (l *ConsulLock) Lock(stopCh <-chan struct{}) (<-chan struct{}, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.unlocked {
		return nil, fmt.Errorf("lock has been unlocked and cannot be reused")
	}

	l.logger.Debug("attempting to acquire consul lock", "key", l.key)

	// Validate and adjust session TTL
	validTTL := validateSessionTTL(l.sessionTTL)
	if validTTL != l.sessionTTL {
		l.logger.Debug("adjusted session TTL to meet Consul requirements",
			"requested", l.sessionTTL, "adjusted", validTTL)
		l.sessionTTL = validTTL
	}

	// Create a session for this lock
	session := &api.SessionEntry{
		TTL:       l.sessionTTL.String(),
		LockDelay: l.lockDelay,
		Behavior:  api.SessionBehaviorRelease,
		Name:      fmt.Sprintf("openbao-lock-%s", l.key),
	}

	sessionID, _, err := l.backend.client.Session().Create(session, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create consul session: %w", err)
	}

	l.session = sessionID
	l.stopCh = make(chan struct{})
	l.doneCh = make(chan struct{})

	// Attempt to acquire the lock
	lockKey := l.backend.consulKey(l.key)

	pair := &api.KVPair{
		Key:     lockKey,
		Value:   []byte(l.value),
		Session: sessionID,
	}

	acquired, _, err := l.backend.kv.Acquire(pair, nil)
	if err != nil {
		// Clean up session on failure
		l.backend.client.Session().Destroy(sessionID, nil)
		l.session = ""
		return nil, fmt.Errorf("failed to acquire consul lock: %w", err)
	}

	if !acquired {
		// Clean up session on failure
		l.backend.client.Session().Destroy(sessionID, nil)
		l.session = ""
		return nil, fmt.Errorf("failed to acquire lock: already held")
	}

	l.logger.Info("successfully acquired consul lock", "key", l.key, "session", sessionID, "ttl", l.sessionTTL)

	// Start session renewal goroutine
	go l.renewSession()

	// Start lock monitoring goroutine
	go l.monitorLock(stopCh)

	return l.doneCh, nil
}

// Unlock releases the lock
func (l *ConsulLock) Unlock() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.logger.Debug("releasing consul lock", "key", l.key)

	if l.session == "" || l.unlocked {
		return nil // Already unlocked
	}

	// Mark as unlocked first
	l.unlocked = true
	sessionID := l.session
	l.session = "" // Clear session ID

	// Signal goroutines to stop
	if l.stopCh != nil {
		close(l.stopCh)
	}

	// Release the lock from Consul
	lockKey := l.backend.consulKey(l.key)
	pair := &api.KVPair{
		Key:     lockKey,
		Session: sessionID,
	}

	released, _, err := l.backend.kv.Release(pair, nil)
	if err != nil {
		l.logger.Error("failed to release consul lock", "error", err)
	} else if !released {
		l.logger.Warn("consul lock was not held when attempting to release", "key", l.key)
	}

	// Destroy the session
	_, sessionErr := l.backend.client.Session().Destroy(sessionID, nil)
	if sessionErr != nil {
		l.logger.Error("failed to destroy consul session", "error", sessionErr)
	}

	// Close the done channel to signal lock release
	if l.doneCh != nil {
		close(l.doneCh)
	}

	l.logger.Info("consul lock released", "key", l.key)

	// Return the more serious error (lock release vs session destroy)
	if err != nil {
		return err
	}
	return sessionErr
}

// Value returns the current lock value
func (l *ConsulLock) Value() (bool, string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// If we've been unlocked or don't have a session, we don't hold the lock
	if l.unlocked || l.session == "" {
		return false, "", nil
	}

	lockKey := l.backend.consulKey(l.key)
	pair, _, err := l.backend.kv.Get(lockKey, nil)
	if err != nil {
		return false, "", err
	}

	// No lock exists
	if pair == nil {
		return false, "", nil
	}

	// Check if the lock is held by our session
	if pair.Session != l.session {
		// Lock exists but not held by us
		return false, string(pair.Value), nil
	}

	// We hold the lock
	return true, string(pair.Value), nil
}

// renewSession periodically renews the Consul session
func (l *ConsulLock) renewSession() {
	ticker := time.NewTicker(l.sessionTTL / 3) // Renew at 1/3 of TTL
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.mu.Lock()
			sessionID := l.session
			unlocked := l.unlocked
			l.mu.Unlock()

			if unlocked || sessionID == "" {
				return
			}

			entry, _, err := l.backend.client.Session().Renew(sessionID, nil)
			if err != nil || entry == nil {
				l.logger.Error("failed to renew consul session", "error", err)
				l.signalLockLost()
				return
			}
			l.logger.Trace("renewed consul session", "session", sessionID)

		case <-l.stopCh:
			return
		}
	}
}

// monitorLock monitors the lock and signals if it's lost
func (l *ConsulLock) monitorLock(stopCh <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Check if we still hold the lock
			held, _, err := l.Value()
			if err != nil {
				l.logger.Error("failed to check lock status", "error", err)
				l.signalLockLost()
				return
			}

			if !held {
				l.logger.Warn("consul lock lost", "key", l.key)
				l.signalLockLost()
				return
			}

		case <-stopCh:
			return

		case <-l.stopCh:
			return
		}
	}
}

// signalLockLost safely signals that the lock has been lost
func (l *ConsulLock) signalLockLost() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.doneCh != nil && !l.unlocked {
		close(l.doneCh)
		l.doneCh = nil
	}
}

// LockWith attempts to acquire a lock for HA coordination
func (c *ConsulBackend) LockWith(key, value string) (physical.Lock, error) {
	if !c.haEnabled {
		return nil, fmt.Errorf("HA not enabled on this backend")
	}

	return &ConsulLock{
		backend:    c,
		key:        key,
		value:      value,
		logger:     c.logger.Named("lock"),
		sessionTTL: c.sessionTTL,
		lockDelay:  c.lockDelay,
	}, nil
}
