package consul

import (
	"crypto/tls"
	"crypto/x509"
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
	"github.com/openbao/openbao/serviceregistration"
)

const (
	defaultServiceName = "openbao"
)

// consulServiceRegistration implements serviceregistration.ServiceRegistration interface
type consulServiceRegistration struct {
	client     *api.Client
	config     *ServiceRegistrationConfig
	logger     hclog.Logger
	serviceID  string
	registered bool
}

// ServiceRegistrationConfig holds the configuration for Consul service registration
type ServiceRegistrationConfig struct {
	Address             string     `hcl:"address"`
	Scheme              string     `hcl:"scheme"`
	Datacenter          string     `hcl:"datacenter"`
	Token               string     `hcl:"token"`
	ServiceName         string     `hcl:"service"`
	ServiceTags         []string   `hcl:"service_tags"`
	ServiceAddress      string     `hcl:"service_address"`
	ServicePort         int        `hcl:"service_port"`
	DisableRegistration bool       `hcl:"disable_registration"`
	MaxRetries          int        `hcl:"max_retries"`
	TLSConfig           *TLSConfig `hcl:"tls"`
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
		Timeout: 5 * time.Second,
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
		client:    client,
		config:    conf,
		logger:    hclogger,
		serviceID: fmt.Sprintf("%s-%d", conf.ServiceName, time.Now().UnixNano()),
	}

	hclogger.Info("consul service registration created", "service_id", csr.serviceID)
	return csr, nil
}

// Run - CRITICAL: This must NOT block!
// Service registration and cleanup are handled in a separate goroutine to prevent blocking the main process.
func (c *consulServiceRegistration) Run(shutdownCh <-chan struct{}, wait *sync.WaitGroup, redirectAddr string) error {
	c.logger.Info("consul service registration Run() called")

	if c.config.DisableRegistration {
		c.logger.Info("consul service registration disabled, returning immediately")
		if wait != nil {
			wait.Done()
		}
		return nil
	}

	// Parse redirect address
	if redirectAddr != "" {
		if err := c.parseRedirectAddr(redirectAddr); err != nil {
			c.logger.Warn("failed to parse redirect address; using configured values", "error", err)
		}
	}

	// Register service in a background goroutine so we don't block
	go func() {
		defer func() {
			// Ensure we always call Done() exactly once
			if wait != nil {
				wait.Done()
			}
		}()

		c.logger.Info("attempting to register service in background")
		if err := c.registerService(); err != nil {
			c.logger.Error("failed to register service. Service will not be available in Consul.", "error", err)
			return // Exit goroutine on registration failure
		}

		// Wait for shutdown signal, then cleanup
		<-shutdownCh
		c.logger.Info("shutdown signal received, deregistering service")
		c.cleanup()
	}()

	c.logger.Info("consul service registration Run() returning immediately. Registration is proceeding in the background.")
	return nil
}

// Notification methods - do nothing
func (c *consulServiceRegistration) NotifyActiveStateChange(isActive bool) error {
	c.logger.Debug("active state changed", "active", isActive)
	return nil
}

func (c *consulServiceRegistration) NotifySealedStateChange(isSealed bool) error {
	c.logger.Debug("sealed state changed", "sealed", isSealed)
	return nil
}

func (c *consulServiceRegistration) NotifyPerformanceStandbyStateChange(isPerformanceStandby bool) error {
	c.logger.Debug("performance standby state changed", "performance_standby", isPerformanceStandby)
	return nil
}

func (c *consulServiceRegistration) NotifyInitializedStateChange(isInitialized bool) error {
	c.logger.Debug("initialized state changed", "initialized", isInitialized)
	return nil
}

// registerService registers the service with Consul
func (c *consulServiceRegistration) registerService() error {
	if c.config.ServicePort == 0 {
		c.logger.Error("service port not specified, cannot register")
		return fmt.Errorf("service port must be specified")
	}

	service := &api.AgentServiceRegistration{
		ID:      c.serviceID,
		Name:    c.config.ServiceName,
		Tags:    c.config.ServiceTags,
		Port:    c.config.ServicePort,
		Address: c.config.ServiceAddress,
		Meta: map[string]string{
			"version": "2.1.0",
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
			c.registered = true
			c.logger.Info("successfully registered service")
			return nil
		}

		c.logger.Warn("failed to register service", "attempt", i+1, "max_attempts", c.config.MaxRetries, "error", lastErr)
		if i < c.config.MaxRetries-1 {
			// Exponential backoff with jitter
			backoffTime := time.Duration(1<<(i+1)) * time.Second
			jitter := time.Duration(100) * time.Millisecond
			time.Sleep(backoffTime + jitter)
		}
	}

	return fmt.Errorf("failed to register after %d attempts: %w", c.config.MaxRetries, lastErr)
}

// cleanup deregisters the service
func (c *consulServiceRegistration) cleanup() {
	if !c.registered {
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
	defer conn.Close()
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
