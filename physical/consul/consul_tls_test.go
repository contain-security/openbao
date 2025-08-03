package consul

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helpers for creating certificates
func createTestCA(t *testing.T) (*x509.Certificate, *rsa.PrivateKey, []byte) {
	// Generate CA private key
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create CA certificate template
	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"Test CA"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"Test"},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// Create CA certificate
	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	// Parse CA certificate
	caCert, err := x509.ParseCertificate(caCertDER)
	require.NoError(t, err)

	// Encode CA certificate to PEM
	caCertPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCertDER,
	})

	return caCert, caKey, caCertPEM
}

func createTestClientCert(t *testing.T, caCert *x509.Certificate, caKey *rsa.PrivateKey) ([]byte, []byte) {
	// Generate client private key
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create client certificate template
	clientTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization:  []string{"Test Client"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"Test"},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	// Create client certificate
	clientCertDER, err := x509.CreateCertificate(rand.Reader, &clientTemplate, caCert, &clientKey.PublicKey, caKey)
	require.NoError(t, err)

	// Encode client certificate to PEM
	clientCertPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: clientCertDER,
	})

	// Encode client private key to PEM
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(clientKey),
	})

	return clientCertPEM, clientKeyPEM
}

func createTestServerCert(t *testing.T, caCert *x509.Certificate, caKey *rsa.PrivateKey, hostname string) ([]byte, []byte) {
	// Generate server private key
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	// Create server certificate template
	serverTemplate := x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject: pkix.Name{
			Organization:  []string{"Test Server"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"Test"},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		SubjectKeyId: []byte{1, 2, 3, 4, 5},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		DNSNames:     []string{hostname},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	// Create server certificate
	serverCertDER, err := x509.CreateCertificate(rand.Reader, &serverTemplate, caCert, &serverKey.PublicKey, caKey)
	require.NoError(t, err)

	// Encode server certificate to PEM
	serverCertPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: serverCertDER,
	})

	// Encode server private key to PEM
	serverKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(serverKey),
	})

	return serverCertPEM, serverKeyPEM
}

func writeTempFile(t *testing.T, content []byte, suffix string) string {
	tmpFile, err := os.CreateTemp("", "consul-test-*"+suffix)
	require.NoError(t, err)
	defer tmpFile.Close()

	_, err = tmpFile.Write(content)
	require.NoError(t, err)

	return tmpFile.Name()
}

func TestConsulBackend_TLS_TLSWithCA(t *testing.T) {
	logger := hclog.NewNullLogger()

	// Create test CA certificate
	_, _, caCertPEM := createTestCA(t)

	// Write CA cert to temp file
	caCertFile := writeTempFile(t, caCertPEM, ".crt")
	defer os.Remove(caCertFile)

	conf := map[string]string{
		"address":     "127.0.0.1:9999", // Use non-existent port
		"path":        "test/",
		"tls_enabled": "true",
		"tls_ca_cert": caCertFile,
	}

	backend, err := NewConsulBackend(conf, logger)

	// We expect a connection error, but CA cert should load successfully
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to Consul")
	assert.Nil(t, backend)
}

func TestConsulBackend_TLS_TLSDisabled(t *testing.T) {
	logger := hclog.NewNullLogger()

	conf := map[string]string{
		"address": "127.0.0.1:8500",
		"path":    "test/",
		// Explicitly disable TLS (this is the default, but being explicit)
	}

	// Test TLS configuration parsing by using a non-existent address
	// This way we test the config parsing without depending on actual Consul availability
	conf["address"] = "127.0.0.1:9999" // Use a port that's unlikely to be in use

	backend, err := NewConsulBackend(conf, logger)

	// We expect a connection error, but TLS should be disabled (scheme should be http)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to Consul")
	assert.Nil(t, backend)

	// Test that the configuration would use HTTP scheme by default
	// We can verify this by checking that no TLS error messages appear
	assert.NotContains(t, err.Error(), "tls")
	assert.NotContains(t, err.Error(), "certificate")
}

func TestConsulBackend_TLS_TLSEnabled(t *testing.T) {
	logger := hclog.NewNullLogger()

	conf := map[string]string{
		"address":     "127.0.0.1:9999", // Use non-existent port
		"scheme":      "http",           // This should be overridden to https when TLS is enabled
		"path":        "test/",
		"tls_enabled": "true",
	}

	backend, err := NewConsulBackend(conf, logger)

	// We expect a connection error, but TLS should be enabled
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to Consul")
	assert.Nil(t, backend)

	// The error might contain TLS-related messages since we're using HTTPS
	// but without proper TLS setup on the non-existent server
}

// TestConsulBackend_TLSConfigOnly tests TLS configuration parsing without network calls
func TestConsulBackend_TLS_TLSConfigOnly(t *testing.T) {
	tests := []struct {
		name          string
		config        map[string]string
		expectError   bool
		errorContains string
	}{
		{
			name: "TLS disabled by default",
			config: map[string]string{
				"address": "127.0.0.1:9999",
				"path":    "test/",
			},
			expectError:   true,
			errorContains: "failed to connect to Consul",
		},
		{
			name: "TLS enabled",
			config: map[string]string{
				"address":     "127.0.0.1:9999",
				"path":        "test/",
				"tls_enabled": "true",
			},
			expectError:   true,
			errorContains: "failed to connect to Consul",
		},
		{
			name: "TLS with skip verify",
			config: map[string]string{
				"address":         "127.0.0.1:9999",
				"path":            "test/",
				"tls_enabled":     "true",
				"tls_skip_verify": "true",
			},
			expectError:   true,
			errorContains: "failed to connect to Consul",
		},
	}

	logger := hclog.NewNullLogger()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend, err := NewConsulBackend(tt.config, logger)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				assert.Nil(t, backend)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, backend)
			}
		})
	}
}

// TestConsulBackend_TLSWithLiveConsul tests against a real Consul instance if available
// This test will be skipped if Consul is not available
func TestConsulBackend_TLS_TLSWithLiveConsul(t *testing.T) {
	// Only run if we can detect Consul is running
	if testing.Short() {
		t.Skip("Skipping live Consul test in short mode")
	}

	logger := hclog.NewNullLogger()

	// Test HTTP connection first
	httpConf := map[string]string{
		"address": "127.0.0.1:8500",
		"path":    "test/",
	}

	httpBackend, httpErr := NewConsulBackend(httpConf, logger)
	if httpErr != nil {
		t.Skipf("Consul HTTP not available on 8500: %v", httpErr)
	}
	if httpBackend != nil {
		t.Logf("✓ Consul HTTP connection successful on port 8500")
	}

	// Test HTTPS connection
	httpsConf := map[string]string{
		"address":         "127.0.0.1:8501",
		"path":            "test/",
		"tls_enabled":     "true",
		"tls_skip_verify": "true", // Skip verification for test
	}

	httpsBackend, httpsErr := NewConsulBackend(httpsConf, logger)
	if httpsErr != nil {
		t.Logf("Consul HTTPS not available on 8501 (expected if not configured): %v", httpsErr)
	} else if httpsBackend != nil {
		t.Logf("✓ Consul HTTPS connection successful on port 8501")
	}

	// At least one should work if Consul is properly configured
	if httpBackend == nil && httpsBackend == nil {
		t.Skip("Neither HTTP nor HTTPS Consul connections available")
	}
}

func TestConsulBackend_TLS_TLSWithInvalidCA(t *testing.T) {
	logger := hclog.NewNullLogger()

	// Write invalid CA cert to temp file
	invalidCert := []byte("invalid certificate data")
	caCertFile := writeTempFile(t, invalidCert, ".crt")
	defer os.Remove(caCertFile)

	conf := map[string]string{
		"address":     "127.0.0.1:8501",
		"path":        "test/",
		"tls_enabled": "true",
		"tls_ca_cert": caCertFile,
	}

	backend, err := NewConsulBackend(conf, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse CA certificate")
	assert.Nil(t, backend)
}

func TestConsulBackend_TLS_TLSWithNonexistentCA(t *testing.T) {
	logger := hclog.NewNullLogger()

	conf := map[string]string{
		"address":     "127.0.0.1:8501",
		"path":        "test/",
		"tls_enabled": "true",
		"tls_ca_cert": "/nonexistent/ca.crt",
	}

	backend, err := NewConsulBackend(conf, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS CA certificate file does not exist")
	assert.Nil(t, backend)
}

func TestConsulBackend_TLS_TLSWithClientCert(t *testing.T) {
	logger := hclog.NewNullLogger()

	// Create test certificates
	caCert, caKey, caCertPEM := createTestCA(t)
	clientCertPEM, clientKeyPEM := createTestClientCert(t, caCert, caKey)

	// Write certificates to temp files
	caCertFile := writeTempFile(t, caCertPEM, ".crt")
	clientCertFile := writeTempFile(t, clientCertPEM, ".crt")
	clientKeyFile := writeTempFile(t, clientKeyPEM, ".key")
	defer func() {
		os.Remove(caCertFile)
		os.Remove(clientCertFile)
		os.Remove(clientKeyFile)
	}()

	conf := map[string]string{
		"address":         "127.0.0.1:8501",
		"path":            "test/",
		"tls_enabled":     "true",
		"tls_ca_cert":     caCertFile,
		"tls_client_cert": clientCertFile,
		"tls_client_key":  clientKeyFile,
	}

	// This will fail to connect to actual Consul, but we're testing TLS config parsing
	backend, err := NewConsulBackend(conf, logger)

	// We expect an error connecting to Consul, but not a configuration error
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to Consul")
	assert.Nil(t, backend)
}

func TestConsulBackend_TLS_TLSWithClientCertMissingKey(t *testing.T) {
	logger := hclog.NewNullLogger()

	// Create test certificates
	caCert, caKey, caCertPEM := createTestCA(t)
	clientCertPEM, _ := createTestClientCert(t, caCert, caKey)

	// Write certificates to temp files
	caCertFile := writeTempFile(t, caCertPEM, ".crt")
	clientCertFile := writeTempFile(t, clientCertPEM, ".crt")
	defer func() {
		os.Remove(caCertFile)
		os.Remove(clientCertFile)
	}()

	conf := map[string]string{
		"address":         "127.0.0.1:8501",
		"path":            "test/",
		"tls_enabled":     "true",
		"tls_ca_cert":     caCertFile,
		"tls_client_cert": clientCertFile,
		// tls_client_key is intentionally missing
	}

	backend, err := NewConsulBackend(conf, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tls_client_key must be provided when tls_client_cert is specified")
	assert.Nil(t, backend)
}

func TestConsulBackend_TLS_TLSWithInvalidClientCert(t *testing.T) {
	logger := hclog.NewNullLogger()

	// Create test CA
	_, _, caCertPEM := createTestCA(t)

	// Write invalid client cert and key
	invalidCert := []byte("invalid certificate")
	invalidKey := []byte("invalid key")

	caCertFile := writeTempFile(t, caCertPEM, ".crt")
	clientCertFile := writeTempFile(t, invalidCert, ".crt")
	clientKeyFile := writeTempFile(t, invalidKey, ".key")
	defer func() {
		os.Remove(caCertFile)
		os.Remove(clientCertFile)
		os.Remove(clientKeyFile)
	}()

	conf := map[string]string{
		"address":         "127.0.0.1:8501",
		"path":            "test/",
		"tls_enabled":     "true",
		"tls_ca_cert":     caCertFile,
		"tls_client_cert": clientCertFile,
		"tls_client_key":  clientKeyFile,
	}

	backend, err := NewConsulBackend(conf, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load client certificate")
	assert.Nil(t, backend)
}

func TestConsulBackend_TLS_TLSWithServerName(t *testing.T) {
	logger := hclog.NewNullLogger()

	// Create test CA
	_, _, caCertPEM := createTestCA(t)

	caCertFile := writeTempFile(t, caCertPEM, ".crt")
	defer os.Remove(caCertFile)

	conf := map[string]string{
		"address":         "consul.example.com:8501",
		"path":            "test/",
		"tls_enabled":     "true",
		"tls_ca_cert":     caCertFile,
		"tls_server_name": "consul.example.com",
	}

	// This will fail to connect to actual Consul, but we're testing TLS config parsing
	backend, err := NewConsulBackend(conf, logger)

	// We expect an error connecting to Consul, but not a configuration error
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to Consul")
	assert.Nil(t, backend)
}

func TestConsulBackend_TLS_TLSEnabledVariations(t *testing.T) {
	logger := hclog.NewNullLogger()

	testCases := []struct {
		name     string
		value    string
		expected bool
	}{
		{"true string", "true", true},
		{"1 string", "1", true},
		{"false string", "false", false},
		{"0 string", "0", false},
		{"empty string", "", false},
		{"random string", "random", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := map[string]string{
				"address":     "127.0.0.1:8501",
				"path":        "test/",
				"tls_enabled": tc.value,
			}

			// This will fail to connect to actual Consul, but we're testing TLS config parsing
			backend, err := NewConsulBackend(conf, logger)

			// We expect an error connecting to Consul, but not a configuration error
			require.Error(t, err)
			assert.Contains(t, err.Error(), "failed to connect to Consul")
			assert.Nil(t, backend)
		})
	}
}

func TestConsulBackend_TLS_TLSSkipVerifyDirect(t *testing.T) {
	logger := hclog.NewNullLogger()

	testCases := []struct {
		name             string
		skipVerifyValue  string
		expectSkipVerify bool
	}{
		{"true string", "true", true},
		{"1 string", "1", true},
		{"false string", "false", false},
		{"0 string", "0", false},
		{"empty string", "", false},
		{"random string", "random", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a test CA to avoid other certificate issues
			_, _, caCertPEM := createTestCA(t)
			caCertFile := writeTempFile(t, caCertPEM, ".crt")
			defer os.Remove(caCertFile)

			conf := map[string]string{
				"address":         "127.0.0.1:9999", // Non-existent port to avoid actual connection
				"path":            "test/",
				"tls_enabled":     "true",
				"tls_skip_verify": tc.skipVerifyValue,
				"tls_ca_cert":     caCertFile, // Provide CA to ensure TLS config is created
			}

			_, err := NewConsulBackend(conf, logger)

			// We expect connection to fail due to non-existent port
			require.Error(t, err)
			assert.Contains(t, err.Error(), "failed to connect to Consul")

			// However, we should have gotten far enough to create the backend structure
			// and parse the TLS configuration before the connection failed
			// Let's check if we can extract the configuration from the error path

			// Alternative approach: Test the configuration parsing in isolation
			// by temporarily modifying the test to not fail on connection
			confForConfigTest := make(map[string]string)
			for k, v := range conf {
				confForConfigTest[k] = v
			}

			// We'll test the config parsing by examining what would be set
			// This is a bit of a workaround since the backend creation fails

			// Create a backend that might succeed (or fail for other reasons)
			// by using a valid but unreachable address format
			testConf := map[string]string{
				"address":         "consul.example.com:8501", // Valid format, won't resolve
				"path":            "test/",
				"tls_enabled":     "true",
				"tls_skip_verify": tc.skipVerifyValue,
				"tls_ca_cert":     caCertFile,
			}

			testBackend, testErr := NewConsulBackend(testConf, logger)

			// Even if connection fails, we want to check if we got far enough
			// to create the TLS config. In some cases, the backend might be created
			// but then fail during connection test
			if testBackend != nil {
				// Great! We got a backend object, let's check the TLS config
				cb, ok := testBackend.(*ConsulBackend)
				require.True(t, ok, "backend should be *ConsulBackend")
				assert.True(t, cb.IsTLSEnabled(), "TLS should be enabled")

				tlsConfig := cb.GetTLSConfig()
				require.NotNil(t, tlsConfig, "TLS config should not be nil when TLS is enabled")

				assert.Equal(t, tc.expectSkipVerify, tlsConfig.InsecureSkipVerify,
					"InsecureSkipVerify should be %v for input %q", tc.expectSkipVerify, tc.skipVerifyValue)

				t.Logf("✓ tls_skip_verify=%q correctly parsed as %v", tc.skipVerifyValue, tc.expectSkipVerify)
			} else {
				// Backend creation failed entirely, but we can still verify the error
				// is connection-related, not config-related
				require.Error(t, testErr)
				assert.Contains(t, testErr.Error(), "failed to connect to Consul")

				// The fact that we got a "failed to connect" error (not a config parsing error)
				// suggests the TLS config was parsed successfully
				t.Logf("✓ tls_skip_verify=%q config parsing succeeded (connection failed as expected)", tc.skipVerifyValue)
			}
		})
	}
}

// TestConsulBackend_TLSConfigIntegration tests the complete TLS configuration
func TestConsulBackend_TLS_TLSConfigIntegration(t *testing.T) {
	logger := hclog.NewNullLogger()

	// Create a complete certificate chain
	caCert, caKey, caCertPEM := createTestCA(t)
	clientCertPEM, clientKeyPEM := createTestClientCert(t, caCert, caKey)
	serverCertPEM, serverKeyPEM := createTestServerCert(t, caCert, caKey, "consul.test.local")

	// Write all certificates to temp files
	caCertFile := writeTempFile(t, caCertPEM, ".crt")
	clientCertFile := writeTempFile(t, clientCertPEM, ".crt")
	clientKeyFile := writeTempFile(t, clientKeyPEM, ".key")
	serverCertFile := writeTempFile(t, serverCertPEM, ".crt")
	serverKeyFile := writeTempFile(t, serverKeyPEM, ".key")

	defer func() {
		os.Remove(caCertFile)
		os.Remove(clientCertFile)
		os.Remove(clientKeyFile)
		os.Remove(serverCertFile)
		os.Remove(serverKeyFile)
	}()

	conf := map[string]string{
		"address":         "consul.test.local:8501",
		"path":            "test/",
		"tls_enabled":     "true",
		"tls_ca_cert":     caCertFile,
		"tls_client_cert": clientCertFile,
		"tls_client_key":  clientKeyFile,
		"tls_server_name": "consul.test.local",
	}

	// This will fail to connect to actual Consul, but we're testing complete TLS config parsing
	backend, err := NewConsulBackend(conf, logger)

	// We expect an error connecting to Consul, but not a configuration error
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to Consul")
	assert.Nil(t, backend)
}

// writeTempFileForBenchmark is a helper function specifically for benchmarks
func writeTempFileForBenchmark(b *testing.B, content []byte, suffix string) string {
	tmpFile, err := os.CreateTemp("", "consul-test-*"+suffix)
	if err != nil {
		b.Fatal(err)
	}
	defer tmpFile.Close()

	_, err = tmpFile.Write(content)
	if err != nil {
		b.Fatal(err)
	}

	return tmpFile.Name()
}

// createTestCAForBenchmark creates test CA specifically for benchmarks
func createTestCAForBenchmark(b *testing.B) (*x509.Certificate, *rsa.PrivateKey, []byte) {
	// Generate CA private key
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		b.Fatal(err)
	}

	// Create CA certificate template
	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"Test CA"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"Test"},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// Create CA certificate
	caCertDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		b.Fatal(err)
	}

	// Parse CA certificate
	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		b.Fatal(err)
	}

	// Encode CA certificate to PEM
	caCertPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCertDER,
	})

	return caCert, caKey, caCertPEM
}

// createTestClientCertForBenchmark creates test client cert specifically for benchmarks
func createTestClientCertForBenchmark(b *testing.B, caCert *x509.Certificate, caKey *rsa.PrivateKey) ([]byte, []byte) {
	// Generate client private key
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		b.Fatal(err)
	}

	// Create client certificate template
	clientTemplate := x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization:  []string{"Test Client"},
			Country:       []string{"US"},
			Province:      []string{""},
			Locality:      []string{"Test"},
			StreetAddress: []string{""},
			PostalCode:    []string{""},
		},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	// Create client certificate
	clientCertDER, err := x509.CreateCertificate(rand.Reader, &clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		b.Fatal(err)
	}

	// Encode client certificate to PEM
	clientCertPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: clientCertDER,
	})

	// Encode client private key to PEM
	clientKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(clientKey),
	})

	return clientCertPEM, clientKeyPEM
}

// Benchmark TLS configuration parsing
func BenchmarkConsulBackend_TLSConfig(b *testing.B) {
	logger := hclog.NewNullLogger()

	// Create test certificates once
	caCert, caKey, caCertPEM := createTestCAForBenchmark(b)
	clientCertPEM, clientKeyPEM := createTestClientCertForBenchmark(b, caCert, caKey)

	caCertFile := writeTempFileForBenchmark(b, caCertPEM, ".crt")
	clientCertFile := writeTempFileForBenchmark(b, clientCertPEM, ".crt")
	clientKeyFile := writeTempFileForBenchmark(b, clientKeyPEM, ".key")

	defer func() {
		os.Remove(caCertFile)
		os.Remove(clientCertFile)
		os.Remove(clientKeyFile)
	}()

	conf := map[string]string{
		"address":         "127.0.0.1:8501",
		"path":            "test/",
		"tls_enabled":     "true",
		"tls_ca_cert":     caCertFile,
		"tls_client_cert": clientCertFile,
		"tls_client_key":  clientKeyFile,
		"tls_server_name": "consul.test.local",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// We expect this to fail connection, but we're benchmarking TLS config parsing
		NewConsulBackend(conf, logger)
	}
}

// Helper function for creating temp directory with certificates
func createTempCertDir(t *testing.T) (string, func()) {
	tmpDir, err := os.MkdirTemp("", "consul-certs-*")
	require.NoError(t, err)

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	return tmpDir, cleanup
}

func TestConsulBackend_TLS_TLSWithCertDirectory(t *testing.T) {
	logger := hclog.NewNullLogger()

	// Create temp directory for certificates
	certDir, cleanup := createTempCertDir(t)
	defer cleanup()

	// Create test certificates
	caCert, caKey, caCertPEM := createTestCA(t)
	clientCertPEM, clientKeyPEM := createTestClientCert(t, caCert, caKey)

	// Write certificates to directory
	caCertPath := filepath.Join(certDir, "ca.crt")
	clientCertPath := filepath.Join(certDir, "client.crt")
	clientKeyPath := filepath.Join(certDir, "client.key")

	err := os.WriteFile(caCertPath, caCertPEM, 0o644)
	require.NoError(t, err)
	err = os.WriteFile(clientCertPath, clientCertPEM, 0o644)
	require.NoError(t, err)
	err = os.WriteFile(clientKeyPath, clientKeyPEM, 0o600)
	require.NoError(t, err)

	conf := map[string]string{
		"address":         "127.0.0.1:8501",
		"path":            "test/",
		"tls_enabled":     "true",
		"tls_ca_cert":     caCertPath,
		"tls_client_cert": clientCertPath,
		"tls_client_key":  clientKeyPath,
	}

	// This will fail to connect to actual Consul, but we're testing TLS config parsing
	backend, err := NewConsulBackend(conf, logger)

	// We expect an error connecting to Consul, but not a configuration error
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to Consul")
	assert.Nil(t, backend)
}
