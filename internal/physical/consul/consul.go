package consul

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-secure-stdlib/parseutil"
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
	haEnabled  bool
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

	// Internal state. The Consul API lock helper owns the session, its
	// periodic renewal and the leadership monitor, so none of that is
	// tracked here.
	mu       sync.Mutex // Protects the fields below
	lock     *api.Lock  // Built on first Lock(), reused after that
	unlocked bool       // Track if we've been unlocked
}

// Defaults for the HA session. defaultSessionTTL is the lifetime of the
// session holding the lock; validateSessionTTL clamps a configured value to
// the range Consul accepts.
//
// lock_delay is the window Consul refuses to re-grant a key after the holder's
// session was invalidated uncleanly. It guards against a former leader that
// has not yet noticed it lost the lock -- paused, partitioned, or simply slow
// to react -- still writing while a successor takes over. It applies only to
// unclean loss: a graceful step-down releases the key explicitly and pays
// nothing.
//
// It stays at Consul's 15s default, which is also what upstream Vault's Consul
// backend uses (it says so where it builds the session: "We use Consul's
// default LockDelay of 15s by not specifying it"). Lowering it would speed up
// unplanned failover, but this backend has nothing else standing between a
// demoted leader and the barrier: upstream pairs that same 15s with write
// fencing -- it implements physical.FencingHABackend and prepends a
// KVCheckSession verb to every write transaction -- and this backend does
// neither, so its writes are unfenced.
//
// The margin is load-bearing rather than nominal. lockMonitorRetries and
// lockMonitorRetryTime below deliberately tolerate transient Consul errors
// before declaring leadership lost, so a former leader can keep believing it
// is active for that long after its session died. The delay has to cover that
// window. Shortening it is safe only once writes are fenced.
//
// Consul caps lock_delay at 60s and enforces that itself when the session is
// created.
const (
	defaultSessionTTL = 15 * time.Second
	defaultLockDelay  = 15 * time.Second
)

// Tuning for the Consul lock helper's leadership monitor. The monitor watches
// the lock key with a blocking query; the helper defaults to zero retries, so
// without these a single transient Consul error would be reported as lost
// leadership and trigger an unnecessary failover.
const (
	lockMonitorRetries   = 5
	lockMonitorRetryTime = 2 * time.Second
)

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
		if _, err := parseutil.ParseDurationSecond(delayStr); err != nil {
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
		if initialInterval, err := parseutil.ParseDurationSecond(initialIntervalStr); err == nil {
			retryConfig.InitialInterval = initialInterval
		}
	}

	if maxIntervalStr := conf["retry_max_interval"]; maxIntervalStr != "" {
		if maxInterval, err := parseutil.ParseDurationSecond(maxIntervalStr); err == nil {
			retryConfig.MaxInterval = maxInterval
		}
	}

	if maxElapsedTimeStr := conf["retry_max_elapsed_time"]; maxElapsedTimeStr != "" {
		if maxElapsedTime, err := parseutil.ParseDurationSecond(maxElapsedTimeStr); err == nil {
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

	// Configure HA. This defaults to on, matching Vault's Consul backend: an
	// operator moving a working storage stanza across would otherwise get a
	// node that never contends for the lock and comes up active and
	// standalone against storage another node is already serving, with
	// nothing in the logs to say so.
	//
	// ParseBool takes strconv's set -- 1/t/T/TRUE/true/True and the false
	// equivalents -- so "yes"/"on"/"no"/"off" are rejected rather than
	// guessed at. That is deliberate: "ha_enabled = no" previously read as
	// false by falling through the old string compare, and silently getting
	// the opposite of a plausible-looking value is the failure this is meant
	// to remove.
	haEnabled := true
	if haStr := conf["ha_enabled"]; haStr != "" {
		parsed, err := parseutil.ParseBool(haStr)
		if err != nil {
			return nil, fmt.Errorf("invalid ha_enabled value %q: %w", haStr, err)
		}
		haEnabled = parsed
	}
	if !haEnabled {
		logger.Warn("HA disabled: node will not contend for leadership and may run active against shared storage",
			"ha_enabled", conf["ha_enabled"])
	}

	// Timing configuration. A value that does not parse is a startup error
	// rather than a silent fallback to the default: a typo in session_ttl or
	// lock_delay changes failover behaviour, and quietly ignoring it hides
	// that until the cluster actually needs to fail over.
	sessionTTL := defaultSessionTTL
	if ttlStr := conf["session_ttl"]; ttlStr != "" {
		parsed, err := parseutil.ParseDurationSecond(ttlStr)
		if err != nil {
			return nil, fmt.Errorf("invalid session_ttl value %q: %w", ttlStr, err)
		}
		sessionTTL = validateSessionTTL(parsed)
		if sessionTTL != parsed {
			logger.Warn("adjusted session_ttl to meet Consul requirements",
				"requested", parsed, "adjusted", sessionTTL)
		}
	} else {
		sessionTTL = validateSessionTTL(sessionTTL)
	}

	lockDelay := defaultLockDelay
	if delayStr := conf["lock_delay"]; delayStr != "" {
		parsed, err := parseutil.ParseDurationSecond(delayStr)
		if err != nil {
			return nil, fmt.Errorf("invalid lock_delay value %q: %w", delayStr, err)
		}
		if parsed < 0 {
			return nil, fmt.Errorf("lock_delay cannot be negative, got %s", parsed)
		}
		// A zero delay cannot be expressed: the session API omits the field
		// when it is zero and Consul then applies its own 15s default, so
		// accepting 0 would hand back the opposite of what was asked for.
		// 1ms is the smallest value that survives the millisecond conversion.
		if parsed == 0 {
			return nil, fmt.Errorf("lock_delay of 0 is not supported because Consul reads an absent delay as its own default; use 1ms for the shortest delay, or omit lock_delay for the default %s", defaultLockDelay)
		}
		lockDelay = parsed
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
		sessionTTL:  sessionTTL,
		lockDelay:   lockDelay,
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
					"consul_key set", (consulKey != ""),
					"error", ctx.Err())
				return ctx.Err()
			}

			return fmt.Errorf("failed to store key %q in consul: %w", entry.Key, err)
		}

		c.logger.Trace("successfully stored key in consul",
			"key", entry.Key,
			"consul_key set", (consulKey != ""),
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
					"consul_key set", (consulKey != ""),
					"error", ctx.Err())
				return ctx.Err()
			}

			return fmt.Errorf("failed to retrieve key %q from consul: %w", key, err)
		}

		// Key doesn't exist
		if pair == nil {
			c.logger.Trace("key not found in consul",
				"key", key,
				"consul_key set", (consulKey != ""))
			result = nil
			return nil
		}

		result = &physical.Entry{
			Key:   key,
			Value: pair.Value,
		}

		c.logger.Trace("successfully retrieved key from consul",
			"key", key,
			"consul_key set", (consulKey != ""),
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
					"consul_key set", (consulKey != ""),
					"error", ctx.Err())
				return ctx.Err()
			}

			return fmt.Errorf("failed to delete key %q from consul: %w", key, err)
		}

		c.logger.Trace("successfully deleted key from consul",
			"key", key,
			"consul_key set", (consulKey != ""))

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
		// physical.Backend.List lists a prefix "up to the next prefix": one
		// level, with anything deeper collapsed to a "subdir/" entry. That is
		// exactly what Consul returns when given "/" as the separator; passing
		// no separator returns the whole key space below the prefix instead.
		queryOpts := &api.QueryOptions{}
		queryOpts = queryOpts.WithContext(ctx)

		keys, _, err := c.kv.Keys(consulPrefix, "/", queryOpts)
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
			if after, ok := strings.CutPrefix(openBaoKey, prefix); ok {
				relativeKey := after
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

	// The cursor is applied by comparison, so the keys have to be ordered.
	// Consul returns them sorted and the prefix strip preserves that, but
	// sorting here keeps the pagination correct regardless.
	sort.Strings(allKeys)

	var result []string
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

		// Skip everything up to and including the cursor. Comparing rather
		// than searching for an exact match matters: the cursor key may have
		// been deleted between pages, and the contract still asks for the keys
		// that sort after it.
		if after != "" && key <= after {
			continue
		}

		// A negative limit means unlimited, per the physical.Backend contract;
		// zero is treated the same way, matching inmem.
		if limit > 0 && len(result) >= limit {
			break
		}

		result = append(result, key)
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
		minTTL = 10 * time.Second // Consul minimum
		maxTTL = 24 * time.Hour   // Consul maximum
	)

	if ttl < minTTL {
		return minTTL
	}
	if ttl > maxTTL {
		return maxTTL
	}
	return ttl
}

// Lock acquires the lock, blocking until it is held or stopCh is closed.
//
// physical.Lock requires Lock to block: core calls it in a retry loop and logs
// every returned error at ERROR, so a non-blocking implementation makes a
// healthy standby emit a continuous stream of "failed to acquire lock: already
// held". Blocking also removes the poll interval from failover, since Consul
// grants a waiting acquire the moment the previous holder's session is released.
//
// A nil channel with a nil error means stopCh fired before the lock was
// acquired; core treats that as "shutting down", matching inmem and raft.
//
// stopCh is only evaluated between blocking queries, so a close can take up to
// the helper's lock wait time (15s by default) to take effect. That bounds how
// long a standby's shutdown can stall waiting for this to return.
func (l *ConsulLock) Lock(stopCh <-chan struct{}) (<-chan struct{}, error) {
	lock, err := l.apiLock()
	if err != nil {
		return nil, err
	}

	l.logger.Debug("attempting to acquire consul lock", "key", l.key)

	// l.mu must NOT be held here: the acquire below blocks for as long as
	// another node holds the lock, and Value() has to stay callable
	// throughout. Unlock() serialises behind a pending acquire because the
	// helper guards both with its own mutex, so an in-flight acquire is
	// aborted with stopCh, not by unlocking.
	leaderCh, err := lock.Lock(stopCh)
	if errors.Is(err, api.ErrLockConflict) {
		// The key exists but is not marked as a Consul lock: almost certainly
		// written by the pre-api.Lock implementation. Clear it if it is unheld
		// and retry once.
		retry, reclaimErr := l.reclaimLegacyLockKey()
		if reclaimErr != nil {
			return nil, fmt.Errorf("%w (while handling %w)", reclaimErr, err)
		}
		if retry {
			leaderCh, err = lock.Lock(stopCh)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to acquire consul lock: %w", err)
	}
	if leaderCh == nil {
		l.logger.Debug("consul lock acquisition interrupted before acquiring", "key", l.key)
		return nil, nil
	}

	l.logger.Info("successfully acquired consul lock", "key", l.key, "ttl", l.sessionTTL)
	return leaderCh, nil
}

// apiLock returns the underlying Consul lock helper, building it on first use.
func (l *ConsulLock) apiLock() (*api.Lock, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.unlocked {
		return nil, fmt.Errorf("lock has been unlocked and cannot be reused")
	}
	if l.lock != nil {
		return l.lock, nil
	}

	// Validate here rather than at config time so a lock built directly in a
	// test cannot install a TTL Consul would reject.
	validTTL := validateSessionTTL(l.sessionTTL)
	if validTTL != l.sessionTTL {
		l.logger.Debug("adjusted session TTL to meet Consul requirements",
			"requested", l.sessionTTL, "adjusted", validTTL)
		l.sessionTTL = validTTL
	}
	ttl := l.sessionTTL.String()
	sessionName := fmt.Sprintf("openbao-lock-%s", l.key)

	lock, err := l.backend.client.LockOpts(&api.LockOptions{
		Key:   l.backend.consulKey(l.key),
		Value: []byte(l.value),
		// SessionOpts drives session creation; SessionTTL is set to the same
		// value because the helper's periodic renewal reads that field rather
		// than SessionOpts.
		SessionTTL: ttl,
		SessionOpts: &api.SessionEntry{
			Name:      sessionName,
			TTL:       ttl,
			LockDelay: l.lockDelay,
			Behavior:  api.SessionBehaviorRelease,
		},
		MonitorRetries:   lockMonitorRetries,
		MonitorRetryTime: lockMonitorRetryTime,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create consul lock: %w", err)
	}

	l.lock = lock
	return lock, nil
}

// Unlock releases the lock and tears down its session.
func (l *ConsulLock) Unlock() error {
	l.mu.Lock()
	lock := l.lock
	if lock == nil || l.unlocked {
		l.mu.Unlock()
		return nil // never locked, or already unlocked
	}
	l.unlocked = true
	l.mu.Unlock()

	l.logger.Debug("releasing consul lock", "key", l.key)

	// Not holding the lock is not an error: the session may already have been
	// invalidated, which is one of the ways leadership is lost.
	//
	// Releasing clears the session from the key and then stops the renewal
	// goroutine, which destroys the session on its way out; session_ttl is
	// only the backstop if that destroy fails. The key itself stays in place
	// with its session cleared, which is the normal Consul lock lifecycle --
	// Value() reports an unheld key as not held.
	if err := lock.Unlock(); err != nil && !errors.Is(err, api.ErrLockNotHeld) {
		return fmt.Errorf("failed to release consul lock: %w", err)
	}

	l.logger.Info("consul lock released", "key", l.key)
	return nil
}

// Value reports whether the lock is held by ANY node, together with the value
// its holder stored.
//
// This is a cluster-wide query, not an ownership check. Core.LeaderLocked
// builds a fresh lock object -- one that never acquired anything and so has no
// session of its own -- and calls Value to learn the active node's UUID before
// reading core/leader/<uuid>. Reporting "held by me" here makes every standby
// answer "no leader", so it can neither serve nor redirect a request. Consul
// represents "held" as a session being attached to the key.
//
// Deliberately takes no mutex: it reads no mutable state, and Lock() blocks for
// as long as another node holds the lock.
func (l *ConsulLock) Value() (bool, string, error) {
	pair, _, err := l.backend.kv.Get(l.backend.consulKey(l.key), nil)
	if err != nil {
		return false, "", fmt.Errorf("failed to read consul lock: %w", err)
	}
	if pair == nil {
		return false, "", nil
	}
	return pair.Session != "", string(pair.Value), nil
}

// reclaimLegacyLockKey clears a lock key left behind by the pre-api.Lock
// implementation so the key can be used as a Consul lock again.
//
// The earlier hand-rolled acquire wrote the lock key with no flags. Consul's
// lock helper marks its keys with LockFlagValue and refuses any key that does
// not carry it (ErrLockConflict), so after an upgrade the node would otherwise
// never be able to take leadership -- the key outlives the session that made
// it, because a released session leaves the key in place.
//
// A key qualifies as reclaimable only when nothing holds it and it carries no
// flags at all, which is the exact signature the old writer left: leftover
// state, not a live lock. The delete is a compare-and-swap on the observed
// ModifyIndex, so if any node acquires the key in the meantime the delete
// fails rather than destroying a real lock. Returns true when the caller
// should retry.
func (l *ConsulLock) reclaimLegacyLockKey() (bool, error) {
	lockKey := l.backend.consulKey(l.key)

	pair, _, err := l.backend.kv.Get(lockKey, nil)
	if err != nil {
		return false, fmt.Errorf("failed to read consul lock: %w", err)
	}
	if pair == nil {
		return true, nil // already gone; retrying is safe
	}
	if pair.Session != "" {
		// Held by someone. Whatever it is, it is live and must not be touched.
		return false, nil
	}
	if pair.Flags != 0 {
		// The legacy writer set no flags at all, so anything else -- a lock
		// key already in the right format, a semaphore, some foreign use of
		// the same path -- is not ours to remove. Retry only when it is
		// already a well-formed lock, which means another node converted it
		// between the helper's read and this one.
		return pair.Flags == api.LockFlagValue, nil
	}

	deleted, _, err := l.backend.kv.DeleteCAS(&api.KVPair{
		Key:         lockKey,
		ModifyIndex: pair.ModifyIndex,
	}, nil)
	if err != nil {
		return false, fmt.Errorf("failed to clear legacy consul lock key: %w", err)
	}
	if !deleted {
		// Someone changed the key first; let the caller retry and re-evaluate.
		return true, nil
	}

	l.logger.Warn("cleared an unheld lock key left by an earlier version so it can be used as a consul lock",
		"key", lockKey, "modify_index", pair.ModifyIndex)
	return true, nil
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
