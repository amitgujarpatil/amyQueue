package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfig holds optional mutual-TLS settings for controller↔broker communication.
// When Enabled=false the HTTP stack is plain — no code-path changes.
type TLSConfig struct {
	Enabled  bool
	CertFile string // PEM certificate (server cert AND client cert)
	KeyFile  string // PEM private key
	CAFile   string // PEM CA — used to verify the peer's certificate
}

// LoadTLSConfig reads TLS settings from environment variables.
// Fails fast if Enabled=true and any required file is missing or unreadable.
func LoadTLSConfig() (*TLSConfig, error) {
	cfg := &TLSConfig{
		Enabled:  getEnvBool("AMYQUEUE_TLS_ENABLED", false),
		CertFile: getEnv("AMYQUEUE_TLS_CERT_FILE", ""),
		KeyFile:  getEnv("AMYQUEUE_TLS_KEY_FILE", ""),
		CAFile:   getEnv("AMYQUEUE_TLS_CA_FILE", ""),
	}

	if !cfg.Enabled {
		return cfg, nil
	}

	for _, f := range []string{cfg.CertFile, cfg.KeyFile, cfg.CAFile} {
		if f == "" {
			return nil, fmt.Errorf("TLS enabled but cert_file, key_file, and ca_file must all be set")
		}
		if _, err := os.Stat(f); err != nil {
			return nil, fmt.Errorf("TLS file not readable %q: %w", f, err)
		}
	}
	return cfg, nil
}

// ServerTLSConfig returns a *tls.Config suitable for an HTTP server that requires
// and verifies client certificates signed by the cluster CA.
func (c *TLSConfig) ServerTLSConfig() (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server cert: %w", err)
	}
	caPool, err := loadCertPool(c.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// ClientTLSConfig returns a *tls.Config suitable for an HTTP client that presents
// its own certificate and verifies the server against the cluster CA.
func (c *TLSConfig) ClientTLSConfig() (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load client cert: %w", err)
	}
	caPool, err := loadCertPool(c.CAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

func loadCertPool(caFile string) (*x509.CertPool, error) {
	data, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file %q: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("no valid certificates found in CA file %q", caFile)
	}
	return pool, nil
}
