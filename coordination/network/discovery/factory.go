package discovery

import (
	"context"
	"fmt"
)

// ProviderType defines supported discovery provider types.
type ProviderType string

const (
	ProviderConsul ProviderType = "consul" // Consul discovery provider
)

// New creates a new instance of ServiceDiscovery based on the given configuration.
//
// Supported providers:
//   - "consul": Uses HashiCorp Consul as the discovery backend.
//
// Parameters:
//   - ctx: Context for cancellation (used for health check registration, etc.).
//   - cfg: Configuration specifying provider type and address.
//
// Returns:
//   - ServiceDiscovery: Concrete implementation (e.g., consulDiscovery).
//   - error: If the provider is unsupported or setup fails.
func New(ctx context.Context, opts *Options) (ServiceDiscovery, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	switch opts.Provider {
	case ProviderConsul:
		return NewConsulDiscovery(opts)
	default:
		return nil, fmt.Errorf("unsupported discovery provider: %s", opts.Provider)
	}
}
