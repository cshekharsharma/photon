package caching

import "context"

// Cache defines a generic caching interface for key-value stores.
//
// This interface abstracts the operations required to interact with a cache backend
// such as Aerospike, Redis, or Memcached. It supports both single and batch operations,
// along with TTL (time-to-live) management and mutation functions.
//
// Implementations of this interface can serve as a plug-and-play cache layer
// in applications, enabling flexible backend swapping without changing the client code.
type Cache interface {

	// Exists checks if the specified key exists in the cache.
	//
	// Parameters:
	//   - request: A pointer to ExistsRequest that includes namespace, collection, and key.
	//
	// Returns:
	//   - bool: True if the key exists, false otherwise.
	//   - error: Any error encountered during the operation.
	Exists(ctx context.Context, request *ExistsRequest) (bool, error)

	// Get retrieves the value for a specific key.
	//
	// Parameters:
	//   - request: A pointer to GetRequest containing the namespace, collection, key, and optional field(s).
	//
	// Returns:
	//   - any: Retrieved value (often map[string]interface{} in systems like Aerospike).
	//   - error: If the key is not found or operation fails.
	Get(ctx context.Context, request *GetRequest) (any, error)

	// Set stores or updates a key-value pair in the cache with optional TTL.
	//
	// Parameters:
	//   - request: A pointer to SetRequest specifying namespace, collection, key, fields, and TTL.
	//
	// Returns:
	//   - bool: True if the operation was successful.
	//   - error: If the operation fails.
	Set(ctx context.Context, request *SetRequest) (bool, error)

	// Delete removes a key from the cache.
	//
	// Parameters:
	//   - request: A pointer to DeleteRequest containing namespace, collection, and key.
	//
	// Returns:
	//   - bool: True if the key was deleted, false if not found.
	//   - error: If deletion fails.
	Delete(ctx context.Context, request *DeleteRequest) (bool, error)

	// MultiGet fetches multiple values for a list of keys.
	//
	// Parameters:
	//   - request: A pointer to MultiGetRequest with a list of keys.
	//
	// Returns:
	//   - map[string]any: A map of keys to their corresponding values.
	//   - error: If the batch fetch fails.
	MultiGet(ctx context.Context, request *MultiGetRequest) (map[string]any, error)

	// MultiSet sets multiple key-value pairs in one batch call.
	//
	// Parameters:
	//   - request: A pointer to MultiSetRequest with a map of keys and their fields.
	//
	// Returns:
	//   - map[string]bool: A map indicating which keys were set successfully.
	//   - error: If the batch set operation fails.
	MultiSet(ctx context.Context, request *MultiSetRequest) (map[string]bool, error)

	// MultiDelete deletes multiple keys from the cache.
	//
	// Parameters:
	//   - request: A pointer to MultiDeleteRequest with a list of keys.
	//
	// Returns:
	//   - map[string]bool: A map of keys to a boolean indicating deletion success.
	//   - error: If the operation encounters an error.
	MultiDelete(ctx context.Context, request *MultiDeleteRequest) (map[string]bool, error)

	// Increment increases integer values of specified fields by given offsets.
	//
	// Parameters:
	//   - request: A pointer to IncrementRequest with field names and increment values.
	//
	// Returns:
	//   - error: If incrementing fails or is applied to non-numeric values.
	Increment(ctx context.Context, request *IncrementRequest) error

	// Decrement decreases integer values of specified fields by given offsets.
	//
	// Parameters:
	//   - request: A pointer to DecrementRequest with field names and decrement values.
	//
	// Returns:
	//   - error: If decrementing fails or is applied to non-numeric values.
	Decrement(ctx context.Context, request *DecrementRequest) error

	// Append appends string values to the specified fields of a key.
	//
	// Parameters:
	//   - request: A pointer to AppendRequest with the data to append.
	//
	// Returns:
	//   - error: If the append operation fails.
	Append(ctx context.Context, request *AppendRequest) error

	// GetTTL retrieves the remaining TTL for a specific key.
	//
	// Parameters:
	//   - request: A pointer to GetTTLRequest specifying the key.
	//
	// Returns:
	//   - int64: TTL in seconds.
	//   - error: If fetching TTL fails.
	GetTTL(ctx context.Context, request *GetTTLRequest) (int64, error)

	// SetTTL updates the TTL of a given key.
	//
	// Parameters:
	//   - request: A pointer to SetTTLRequest with the new TTL.
	//
	// Returns:
	//   - error: If updating the TTL fails.
	SetTTL(ctx context.Context, request *SetTTLRequest) error
}
