package consul

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-secure-stdlib/parseutil"
	"github.com/openbao/openbao/v2/internal/serviceregistration"
	"github.com/openbao/openbao/v2/internal/version"
)

const (
	defaultServiceName = "openbao"

	// Tag vocabulary, matching Vault's Consul registration so existing
	// active.<service>.service.consul / standby.<service>.service.consul DNS
	// and any tag-based load balancer routing keeps working across a
	// migration.
	tagActive             = "active"
	tagStandby            = "standby"
	tagPerformanceStandby = "performance-standby"
	tagInitialized        = "initialized"
	tagSealed             = "sealed"

	// The health check is a TTL check this process refreshes, rather than one
	// the Consul agent polls: it needs no reachable API address or TLS
	// material, and it reports critical if the process wedges or dies, not
	// merely if a port stops answering.
	defaultCheckTimeout = 5 * time.Second
	minCheckTimeout     = time.Second
	maxCheckTimeout     = 5 * time.Minute

	// Ceiling on the registration retry backoff. Unbounded doubling with a
	// large max_retries would leave shutdown blocked for minutes.
	maxRegisterBackoff = 30 * time.Second

	// Cap on the doubling itself, so a large max_retries cannot overflow the
	// duration into a negative value and turn the retry loop into a hot spin.
	// Lossless as long as 1<<maxBackoffShift seconds >= maxRegisterBackoff,
	// which registerBackoff's test asserts.
	maxBackoffShift = 5

	// Timeout on the HTTP client used to talk to the Consul agent. The check
	// TTL is derived from it: a refresh that is merely slow must not be able
	// to outlive the TTL and flap a healthy node out of the catalog.
	consulHTTPTimeout = 5 * time.Second
)

// consulServiceRegistration implements serviceregistration.ServiceRegistration interface
type consulServiceRegistration struct {
	client *api.Client
	config *ServiceRegistrationConfig
	logger hclog.Logger
	// serviceID and checkID are derived in Run, once the redirect address has
	// been applied. They are deterministic in the service name, address and
	// port so a restarted node overwrites its own catalog entry rather than
	// leaving a stale one behind under a fresh id.
	serviceID string
	checkID   string

	// mu guards the fields below, which the notification callbacks write and
	// the maintenance goroutine reads.
	mu         sync.Mutex
	registered bool
	state      serviceregistration.State
	// needsRegister records that the catalog is out of step with state --
	// either nothing has been registered yet, or a publish failed. The
	// maintenance loop reconciles it on every tick, so a failed update
	// cannot strand the node with stale tags until the next notification.
	needsRegister bool

	// notifyCh carries a single pending "state changed" signal to the
	// maintenance goroutine. It is buffered and written without blocking, so
	// a burst of notifications collapses into one refresh.
	notifyCh chan struct{}
}

// ServiceRegistrationConfig holds the configuration for Consul service registration
type ServiceRegistrationConfig struct {
	Address             string        `hcl:"address"`
	Scheme              string        `hcl:"scheme"`
	Datacenter          string        `hcl:"datacenter"`
	Token               string        `hcl:"token"`
	ServiceName         string        `hcl:"service"`
	ServiceTags         []string      `hcl:"service_tags"`
	ServiceAddress      string        `hcl:"service_address"`
	ServicePort         int           `hcl:"service_port"`
	DisableRegistration bool          `hcl:"disable_registration"`
	MaxRetries          int           `hcl:"max_retries"`
	CheckTimeout        time.Duration `hcl:"check_timeout"`

	TLSConfig *TLSConfig `hcl:"tls"`
}

// TLSConfig holds TLS configuration for Consul connection
type TLSConfig struct {
	CertFile           string `hcl:"tls_cert_file"`
	KeyFile            string `hcl:"tls_key_file"`
	CAFile             string `hcl:"tls_ca_file"`
	CAPath             string `hcl:"tls_ca_path"`
	ServerName         string `hcl:"tls_server_name"`
	InsecureSkipVerify bool   `hcl:"tls_skip_verify"`
}

// ParseServiceRegistrationConfig parses a service_registration stanza from OpenBao config
func ParseServiceRegistrationConfig(configMap map[string]string) (*ServiceRegistrationConfig, error) {
	if configMap == nil {
		return nil, fmt.Errorf("config map is nil")
	}

	config := &ServiceRegistrationConfig{}

	// Parse disable_registration first
	if val, exists := configMap["disable_registration"]; exists {
		if disableReg, err := parseBoolFromString(val); err != nil {
			return nil, fmt.Errorf("failed to parse 'disable_registration' as a boolean: %w", err)
		} else if disableReg {
			return nil, nil // Return nil if registration is disabled
		}
	}

	// Parse basic fields
	config.Address = strings.TrimSpace(configMap["address"])
	config.Scheme = strings.TrimSpace(configMap["scheme"])
	config.Datacenter = strings.TrimSpace(configMap["datacenter"])
	config.Token = strings.TrimSpace(configMap["token"])

	// Set default service name if not provided
	config.ServiceName = strings.TrimSpace(configMap["service"])
	if config.ServiceName == "" {
		config.ServiceName = defaultServiceName
	}

	config.ServiceAddress = strings.TrimSpace(configMap["service_address"])

	// Parse service_tags
	if val, exists := configMap["service_tags"]; exists && val != "" {
		tags := strings.Split(val, ",")
		for i, tag := range tags {
			tags[i] = strings.TrimSpace(tag)
		}
		config.ServiceTags = tags
	}

	// Parse service_port
	if val, exists := configMap["service_port"]; exists {
		if port, err := parseIntFromString(val); err != nil {
			return nil, fmt.Errorf("failed to parse 'service_port' as an integer: %w", err)
		} else {
			config.ServicePort = port
		}
	}

	// Parse check_timeout, the interval at which the TTL health check is
	// refreshed. An unparseable value is a startup error rather than a silent
	// fallback: it decides how quickly a wedged node is marked unhealthy.
	config.CheckTimeout = defaultCheckTimeout
	if val, exists := configMap["check_timeout"]; exists && val != "" {
		timeout, err := parseutil.ParseDurationSecond(val)
		if err != nil {
			return nil, fmt.Errorf("failed to parse 'check_timeout' as a duration: %w", err)
		}
		if timeout < minCheckTimeout || timeout > maxCheckTimeout {
			// An unbounded upper end would let a wedged process stay healthy
			// for as long as the TTL, which is the failure this check exists
			// to catch.
			return nil, fmt.Errorf("check_timeout must be between %s and %s, got %s", minCheckTimeout, maxCheckTimeout, timeout)
		}
		config.CheckTimeout = timeout
	}

	// Parse max_retries
	if val, exists := configMap["max_retries"]; exists {
		if retries, err := parseIntFromString(val); err != nil {
			return nil, fmt.Errorf("failed to parse 'max_retries' as an integer: %w", err)
		} else {
			config.MaxRetries = retries
		}
	}

	// Retain the tls_enabled check as requested
	if val, exists := configMap["tls_enabled"]; exists {
		if tlsEnabled, err := parseBoolFromString(val); err != nil {
			return nil, fmt.Errorf("failed to parse 'tls_enabled' as a boolean: %w", err)
		} else if tlsEnabled {
			tlsConfig, err := parseTLSConfig(configMap)
			if err != nil {
				return nil, fmt.Errorf("failed to parse TLS config: %w", err)
			}
			config.TLSConfig = tlsConfig
		}
	}

	return config, nil
}

// parseTLSConfig parses TLS-related fields from the config map
func parseTLSConfig(configMap map[string]string) (*TLSConfig, error) {
	tlsConfig := &TLSConfig{}

	tlsConfig.CertFile = configMap["tls_client_cert"]
	tlsConfig.KeyFile = configMap["tls_client_key"]
	tlsConfig.CAFile = configMap["tls_ca_cert"]
	tlsConfig.CAPath = configMap["tls_ca_path"]
	tlsConfig.ServerName = configMap["tls_server_name"]

	if val, exists := configMap["tls_skip_verify"]; exists {
		if skip, err := parseBoolFromString(val); err != nil {
			return nil, fmt.Errorf("invalid tls_skip_verify value: %v", err)
		} else {
			tlsConfig.InsecureSkipVerify = skip
		}
	}

	return tlsConfig, nil
}

// Helper functions
func parseBoolFromString(val string) (bool, error) {
	cleanStr := strings.Trim(val, `"`)
	return strconv.ParseBool(cleanStr)
}

func parseIntFromString(val string) (int, error) {
	cleanStr := strings.Trim(val, `"`)
	return strconv.Atoi(cleanStr)
}

// NewConsulServiceRegistration creates a new Consul service registration instance
func NewConsulServiceRegistration(config map[string]string, hclogger hclog.Logger, state serviceregistration.State) (serviceregistration.ServiceRegistration, error) {
	if hclogger == nil {
		hclogger = hclog.NewNullLogger()
	}

	hclogger.Info("creating consul service registration")

	// Parse configuration
	conf, err := ParseServiceRegistrationConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse consul service registration config: %w", err)
	}

	if conf == nil {
		hclogger.Info("consul service registration disabled")
		return &consulServiceRegistration{logger: hclogger, config: &ServiceRegistrationConfig{DisableRegistration: true}}, nil
	}

	// Set remaining defaults here
	if conf.Address == "" {
		conf.Address = "127.0.0.1:8500"
	}
	if conf.Scheme == "" {
		conf.Scheme = "http"
	}
	if conf.MaxRetries == 0 {
		conf.MaxRetries = 3
	}

	hclogger.Info("consul config parsed",
		"address", conf.Address,
		"scheme", conf.Scheme,
		"service_name", conf.ServiceName,
		"service_port", conf.ServicePort)

	// Create Consul client config
	clientConfig := api.DefaultConfig()
	clientConfig.Address = conf.Address
	clientConfig.Scheme = conf.Scheme
	clientConfig.Datacenter = conf.Datacenter
	clientConfig.Token = conf.Token

	// Create HTTP client with timeout
	httpClient := &http.Client{
		Timeout: consulHTTPTimeout,
	}

	// Configure TLS if provided
	if conf.TLSConfig != nil {
		hclogger.Info("configuring TLS", "skip_verify", conf.TLSConfig.InsecureSkipVerify)

		tlsConfig, err := setupTLS(conf.TLSConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to setup TLS: %w", err)
		}

		transport := &http.Transport{
			TLSClientConfig: tlsConfig,
			DialContext: (&net.Dialer{
				Timeout: 5 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout: 5 * time.Second,
		}

		httpClient.Transport = transport
		hclogger.Info("TLS configured")
	}

	clientConfig.HttpClient = httpClient

	// Create Consul client
	client, err := api.NewClient(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consul client: %w", err)
	}

	// Auto-detect service address if not provided
	if conf.ServiceAddress == "" && conf.ServicePort > 0 {
		if addr, err := detectLocalAddress(); err == nil {
			conf.ServiceAddress = addr
		}
	}

	csr := &consulServiceRegistration{
		client: client,
		config: conf,
		logger: hclogger,
		// Seed from the state Core built us with, so the very first
		// registration already carries the right tags and check status
		// instead of advertising a default until the first notification.
		state:    state,
		notifyCh: make(chan struct{}, 1),
	}

	hclogger.Info("consul service registration created", "service", conf.ServiceName)
	return csr, nil
}

// Run - CRITICAL: This must NOT block!
// Service registration and cleanup are handled in a separate goroutine to prevent blocking the main process.
func (c *consulServiceRegistration) Run(shutdownCh <-chan struct{}, wait *sync.WaitGroup, redirectAddr string) error {
	c.logger.Info("consul service registration Run() called")

	if c.config.DisableRegistration {
		c.logger.Info("consul service registration disabled, returning immediately")
		return nil
	}

	// Parse redirect address
	if redirectAddr != "" {
		if err := c.parseRedirectAddr(redirectAddr); err != nil {
			c.logger.Warn("failed to parse redirect address; using configured values", "error", err)
		}
	}

	// Derive the ids now: parseRedirectAddr above may have supplied the
	// address and port they are built from.
	c.setIDs()

	// Register in the background so Run does not block startup. wait.Go both
	// registers the goroutine with the caller's WaitGroup and marks it done,
	// which is what the caller expects: Core builds one shared WaitGroup and
	// only ever Waits on it, so calling Done against it would drive the
	// counter negative and panic.
	runBackground := func(fn func()) { go fn() }
	if wait != nil {
		runBackground = wait.Go
	}

	runBackground(func() {
		c.logger.Info("attempting to register service in background")
		if err := c.registerService(shutdownCh); err != nil {
			// Not fatal: the maintenance loop reconciles, so a Consul that is
			// slow to come up does not cost this node its registration for
			// the rest of the process lifetime.
			c.logger.Error("failed to register service, will retry", "error", err)
		}

		c.maintain(shutdownCh)

		c.logger.Info("shutdown signal received, deregistering service")
		c.cleanup()
	})

	c.logger.Info("consul service registration Run() returning immediately. Registration is proceeding in the background.")
	return nil
}

// checkTTL is the lifetime registered for the health check.
//
// It has to cover more than the refresh interval: a refresh that merely blocks
// on a slow agent, up to the HTTP client timeout, must not be able to outlive
// the TTL and flap a healthy node out of the catalog. Three refresh intervals
// covers the ordinary case; the second term covers a short check_timeout,
// where three intervals could be shorter than a single stalled request.
func (c *consulServiceRegistration) checkTTL() time.Duration {
	interval := c.config.CheckTimeout
	ttl := 3 * interval
	if floor := consulHTTPTimeout + 2*interval; ttl < floor {
		ttl = floor
	}
	return ttl
}

// setIDs derives the Consul service and check ids.
//
// They are deterministic in the service name, address and port rather than
// unique per process start: a restart then re-registers over the same entry,
// so a crashed node leaves no orphan behind and a sealed node can be left
// visibly critical in the catalog instead of being reaped out of it.
func (c *consulServiceRegistration) setIDs() {
	addr := c.config.ServiceAddress
	if addr == "" {
		addr = "local"
	}
	// Colons appear in IPv6 literals and separate the check id suffix.
	addr = strings.ReplaceAll(addr, ":", "-")

	c.serviceID = fmt.Sprintf("%s-%s-%d", c.config.ServiceName, addr, c.config.ServicePort)
	c.checkID = c.serviceID + ":seal-status"
	c.logger.Info("consul service registration ids", "service_id", c.serviceID, "check_id", c.checkID)
}

// serviceTags renders the node's state as the tag set Vault publishes, with
// any statically configured tags appended.
//
// active/standby is what active.<service>.service.consul and its standby
// counterpart resolve on, and what tag-routing load balancers select against;
// a node that never re-tags is either invisible to that routing or, worse,
// still advertised as active after it has become a standby.
func (c *consulServiceRegistration) serviceTags(state serviceregistration.State) []string {
	tags := make([]string, 0, len(c.config.ServiceTags)+4)
	if state.IsActive {
		tags = append(tags, tagActive)
	} else {
		tags = append(tags, tagStandby)
	}
	if state.IsPerformanceStandby {
		tags = append(tags, tagPerformanceStandby)
	}
	if state.IsInitialized {
		tags = append(tags, tagInitialized)
	}
	if state.IsSealed {
		tags = append(tags, tagSealed)
	}
	return append(tags, c.config.ServiceTags...)
}

// checkStatus maps seal state onto the health check. A sealed node can serve
// nothing, so it must not stay in the healthy set a load balancer draws from.
func checkStatus(state serviceregistration.State) string {
	if state.IsSealed {
		return api.HealthCritical
	}
	return api.HealthPassing
}

// updateState applies a change and wakes the maintenance goroutine. It never
// talks to Consul itself: Core only logs a warning when these return an error,
// so retrying is this implementation's responsibility and belongs on the
// goroutine that already has a retry loop.
func (c *consulServiceRegistration) updateState(mutate func(*serviceregistration.State)) error {
	if c.config.DisableRegistration {
		return nil
	}

	c.mu.Lock()
	mutate(&c.state)
	c.needsRegister = true
	c.mu.Unlock()

	// Buffered, so a burst of changes collapses into a single refresh rather
	// than blocking Core's notification path.
	select {
	case c.notifyCh <- struct{}{}:
	default:
	}
	return nil
}

func (c *consulServiceRegistration) NotifyActiveStateChange(isActive bool) error {
	c.logger.Debug("active state changed", "active", isActive)
	return c.updateState(func(s *serviceregistration.State) { s.IsActive = isActive })
}

func (c *consulServiceRegistration) NotifySealedStateChange(isSealed bool) error {
	c.logger.Debug("sealed state changed", "sealed", isSealed)
	return c.updateState(func(s *serviceregistration.State) { s.IsSealed = isSealed })
}

func (c *consulServiceRegistration) NotifyPerformanceStandbyStateChange(isPerformanceStandby bool) error {
	c.logger.Debug("performance standby state changed", "performance_standby", isPerformanceStandby)
	return c.updateState(func(s *serviceregistration.State) { s.IsPerformanceStandby = isPerformanceStandby })
}

func (c *consulServiceRegistration) NotifyInitializedStateChange(isInitialized bool) error {
	c.logger.Debug("initialized state changed", "initialized", isInitialized)
	return c.updateState(func(s *serviceregistration.State) { s.IsInitialized = isInitialized })
}

// registerBackoff is how long to wait before retry number attempt+1.
//
// The doubling is capped as well as the result: without that, a large
// max_retries overflows the duration into a negative value, which fires the
// timer immediately and turns the retry into an unthrottled loop against the
// Consul client.
func registerBackoff(attempt int) time.Duration {
	shift := min(attempt+1, maxBackoffShift)
	return min(time.Duration(1<<shift)*time.Second, maxRegisterBackoff) + 100*time.Millisecond
}

// errNoServicePort is permanent: no amount of retrying supplies a port, so
// callers stop rather than logging the same failure every interval.
var errNoServicePort = errors.New("service port must be specified")

// registerService publishes the service and its health check, retrying a
// transient Consul failure with a capped backoff. It must be called only
// after setIDs, which Run does before starting the maintenance goroutine.
func (c *consulServiceRegistration) registerService(shutdownCh <-chan struct{}) error {
	if c.config.ServicePort == 0 {
		return errNoServicePort
	}

	c.mu.Lock()
	state := c.state
	c.mu.Unlock()

	service := &api.AgentServiceRegistration{
		ID:      c.serviceID,
		Name:    c.config.ServiceName,
		Tags:    c.serviceTags(state),
		Port:    c.config.ServicePort,
		Address: c.config.ServiceAddress,
		Meta: map[string]string{
			"version": version.GetVersion().VersionNumber(),
		},
		Check: &api.AgentServiceCheck{
			CheckID: c.checkID,
			Name:    "OpenBao Seal Status",
			Notes:   "OpenBao is healthy when unsealed, so it can serve or become the active node",
			Status:  checkStatus(state),
			TTL:     c.checkTTL().String(),
			// Deliberately no DeregisterCriticalServiceAfter: a sealed node
			// is critical by design, and reaping it would delete the node
			// from the catalog exactly when an operator needs to see that it
			// is up but sealed. Stale entries are handled by the ids being
			// deterministic, so a restart overwrites rather than accumulates.
		},
	}

	c.logger.Info("registering service",
		"id", c.serviceID,
		"name", c.config.ServiceName,
		"address", c.config.ServiceAddress,
		"port", c.config.ServicePort)

	// Try to register with retries and exponential backoff
	var lastErr error
	for i := 0; i < c.config.MaxRetries; i++ {
		lastErr = c.client.Agent().ServiceRegister(service)
		if lastErr == nil {
			c.mu.Lock()
			c.registered = true
			c.mu.Unlock()
			c.logger.Info("successfully registered service", "tags", service.Tags, "check_status", service.Check.Status)
			return nil
		}

		c.logger.Warn("failed to register service", "attempt", i+1, "max_attempts", c.config.MaxRetries, "error", lastErr)
		if i < c.config.MaxRetries-1 {
			// Capped exponential backoff, interruptible: this runs on the
			// goroutine shutdown waits for, so an uninterruptible sleep here
			// stalls the whole process exit.
			timer := time.NewTimer(registerBackoff(i))
			select {
			case <-timer.C:
			case <-shutdownCh:
				timer.Stop()
				return fmt.Errorf("shutting down before the service could be registered: %w", lastErr)
			}
		}
	}

	return fmt.Errorf("failed to register after %d attempts: %w", c.config.MaxRetries, lastErr)
}

// maintain keeps the registration current until shutdown.
//
// It has two jobs: refresh the TTL check so Consul keeps considering this node
// alive, and re-register whenever the node's state changes so its tags and
// check status match reality. Both retry, because the ServiceRegistration
// contract puts the burden of retrying on the implementation -- Core only logs
// a warning when a notification returns an error.
func (c *consulServiceRegistration) maintain(shutdownCh <-chan struct{}) {
	ticker := time.NewTicker(c.config.CheckTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-shutdownCh:
			return
		case <-c.notifyCh:
			// State changed: publish it immediately rather than waiting for
			// the next tick.
		case <-ticker.C:
		}

		if !c.reconcile(shutdownCh) {
			return
		}
	}
}

// reconcile brings the catalog back in line with the current state and
// refreshes the health check. It returns false when the process is shutting
// down and the loop should stop.
func (c *consulServiceRegistration) reconcile(shutdownCh <-chan struct{}) bool {
	c.mu.Lock()
	needsRegister := c.needsRegister || !c.registered
	// Cleared before publishing, never after. registerService reads the state
	// once and then talks to Consul; a change landing in that window must not
	// be mistaken for one this publish already covered, or the node keeps
	// stale tags -- still advertising itself active after stepping down --
	// until some later notification happens along.
	c.needsRegister = false
	c.mu.Unlock()

	if needsRegister {
		// ServiceRegister is an upsert on the service id, so this both
		// creates the registration and updates the tags and check status of
		// an existing one.
		if err := c.registerService(shutdownCh); err != nil {
			c.mu.Lock()
			c.needsRegister = true
			c.mu.Unlock()

			if errors.Is(err, errNoServicePort) {
				c.logger.Error("cannot register service, giving up", "error", err)
				return false
			}
			c.logger.Warn("failed to publish service registration, will retry", "error", err)
			return !isClosed(shutdownCh)
		}
	}

	c.updateCheck()
	return true
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// updateCheck refreshes the TTL check with the current seal status. Letting it
// lapse instead would mark the node critical, which is the correct outcome
// when the process is genuinely stuck but wrong for a transient Consul error.
func (c *consulServiceRegistration) updateCheck() {
	c.mu.Lock()
	state := c.state
	registered := c.registered
	c.mu.Unlock()

	if !registered {
		return
	}

	status := checkStatus(state)
	notes := fmt.Sprintf("sealed=%t active=%t initialized=%t", state.IsSealed, state.IsActive, state.IsInitialized)
	if err := c.client.Agent().UpdateTTL(c.checkID, notes, status); err != nil {
		// The check can be missing entirely -- an agent that was restarted or
		// reprovisioned, or an operator deregistering by hand. Re-register on
		// the next pass rather than warning about the same missing check
		// forever.
		c.logger.Warn("failed to refresh consul health check, will re-register", "check_id", c.checkID, "error", err)
		c.mu.Lock()
		c.needsRegister = true
		c.mu.Unlock()
	}
}

// cleanup deregisters the service
func (c *consulServiceRegistration) cleanup() {
	c.mu.Lock()
	registered := c.registered
	c.registered = false
	c.mu.Unlock()

	if !registered {
		return
	}

	c.logger.Info("deregistering service", "service_id", c.serviceID)
	if err := c.client.Agent().ServiceDeregister(c.serviceID); err != nil {
		c.logger.Error("failed to deregister service", "error", err)
	} else {
		c.logger.Info("successfully deregistered service")
	}
}

func setupTLS(config *TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		ServerName:         config.ServerName,
		InsecureSkipVerify: config.InsecureSkipVerify,
	}

	// Load client certificate if provided
	if config.CertFile != "" && config.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Load CA certificates if provided
	if config.CAFile != "" || config.CAPath != "" {
		certPool := x509.NewCertPool()

		if config.CAFile != "" {
			caCert, err := os.ReadFile(config.CAFile)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA file: %w", err)
			}
			if ok := certPool.AppendCertsFromPEM(caCert); !ok {
				return nil, fmt.Errorf("failed to append CA file to cert pool")
			}
		}

		if config.CAPath != "" {
			err := filepath.Walk(config.CAPath, func(path string, info os.FileInfo, err error) error {
				if err != nil || info.IsDir() || !strings.HasSuffix(info.Name(), ".pem") {
					return err
				}
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				certPool.AppendCertsFromPEM(data)
				return nil
			})
			if err != nil {
				return nil, err
			}
		}

		tlsConfig.RootCAs = certPool
	}

	return tlsConfig, nil
}

func detectLocalAddress() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "", err
	}
	// Nothing is ever sent on this probe socket, so Close cannot fail meaningfully.
	defer func() { _ = conn.Close() }()
	return conn.LocalAddr().(*net.UDPAddr).IP.String(), nil
}

func (c *consulServiceRegistration) parseRedirectAddr(redirectAddr string) error {
	redirectAddr = strings.TrimPrefix(redirectAddr, "https://")
	redirectAddr = strings.TrimPrefix(redirectAddr, "http://")

	host, portStr, err := net.SplitHostPort(redirectAddr)
	if err != nil {
		return err
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return err
	}

	if c.config.ServiceAddress == "" {
		c.config.ServiceAddress = host
	}
	if c.config.ServicePort == 0 {
		c.config.ServicePort = port
	}

	return nil
}
