package discovery

import (
	"errors"
	"net/http"
	"time"

	"github.com/cshekharsharma/photon/core/logger"
)

// Options defines the configuration parameters required to initialize
// a service discovery provider such as Consul, Etcd, etc.
type Options struct {
	Provider   ProviderType  // Provider specifies the type of service discovery backend (e.g., Consul, Etcd).
	Address    string        // Address is the endpoint URL of the discovery provider
	Token      string        // Token is an optional ACL token for authenticating with the discovery provider.
	Scheme     string        // Scheme is the URL scheme to use (e.g., "http" or "https").
	Datacenter string        // Datacenter specifies the target datacenter to use for lookups and registration.
	HTTPClient *http.Client  // HTTPClient allows the use of a custom HTTP client.
	TLSConfig  *TLSConfig    // TLSConfig contains optional TLS settings used when communicating over HTTPS.
	WaitTime   time.Duration // WaitTime is the maximum duration to wait for blocking queries (e.g., in Watch).
	Logger     logger.Logger // Logger for logging messages and errors.
}

// Validate checks if the required fields in Options are set.
// It returns an error if any required field is missing or invalid.
func (o *Options) Validate() error {
	if o == nil {
		return errors.New("options are required")
	}
	if o.Provider == "" {
		return errors.New("provider type is required")
	}
	if o.Address == "" {
		return errors.New("address is required")
	}
	if o.Logger == nil {
		return errors.New("logger is required")
	}
	if o.TLSConfig != nil && o.TLSConfig.InsecureSkipVerify && !o.TLSConfig.AllowInsecureTLS {
		return errors.New("insecure TLS verification requires AllowInsecureTLS")
	}
	return nil
}

// TLSConfig contains security parameters used for mutual TLS authentication
// and secure communication with the service discovery backend.
type TLSConfig struct {
	CAFile             string // path to the CA certificate used to verify the server's certificate.
	CertFile           string // path to the client's TLS certificate.
	KeyFile            string // path to the client's TLS private key.
	InsecureSkipVerify bool   // if true, disables verification of the server's certificate.
	AllowInsecureTLS   bool   // must be true to permit InsecureSkipVerify.
}
