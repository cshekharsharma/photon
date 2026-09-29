package caching

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	aerov8 "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/cshekharsharma/photon/storage/aerospike"
)

// AerospikeCache provides a high-level caching interface over the Aerospike key-value store.
//
// This struct abstracts operations such as Get, Set, Exists, Delete, Increment, Decrement,
// and TTL management by internally using an Aerospike client that adheres to the AerospikeInterface.
//
// Fields:
//   - aero: An implementation of the AerospikeInterface that handles low-level interactions
//     with the Aerospike database, including connection management and operations.
//   - namespace: A logical grouping of data in Aerospike. Similar to a database in relational systems.
//   - collection: Represents the 'set' within the namespace, used for organizing related records,
//     similar to a table in relational databases.
//
// Typical usage includes initializing this struct via NewAerospikeCache, and then using its
// methods to interact with the underlying Aerospike store in a type-safe, request-driven manner.
type AerospikeCache struct {
	aero       aerospike.AerospikeInterface
	namespace  string
	collection string
}

var (
	aerospikeConnect = aerospike.Connect
	aerospikeNewKey  = aerospike.NewKey
	aerospikeNewKeys = aerospike.NewMultipleKey
)

// NewAerospikeCache creates a new AerospikeCache instance using the provided options.
// It establishes a connection with the Aerospike server based on the specified cluster.
func NewAerospikeCache(opts *Options, connector aerospike.AerospikeConnectorInterface) (*AerospikeCache, error) {
	sanitizedTTL, err := aerospikeTTL(opts.DefaultTTL)
	if err != nil {
		return nil, err
	}

	aerospike.SetConnectionConfig(opts.Cluster, &aerospike.ConnectionConfig{
		Hosts:             opts.Hosts,
		ConnectionTimeout: opts.ConnTimeout,
		DefaultTTL:        sanitizedTTL,
	})

	if connector == nil {
		connector = &aerospike.AerospikeConnector{}
	}

	aero, err := aerospikeConnect(connector, opts.Cluster)

	if err != nil {
		return nil, err
	}

	aerocache := &AerospikeCache{
		aero:       aero,
		namespace:  opts.Namespace,
		collection: opts.Collection,
	}

	return aerocache, nil
}

func aerospikeReadPolicyForContext(ctx context.Context) *aerov8.BasePolicy {
	policy := aerov8.NewPolicy()
	applyAerospikeDeadline(ctx, policy)
	return policy
}

func aerospikeBatchPolicyForContext(ctx context.Context) *aerov8.BatchPolicy {
	policy := aerov8.NewBatchPolicy()
	applyAerospikeDeadline(ctx, &policy.BasePolicy)
	return policy
}

func aerospikeWritePolicyForContext(ctx context.Context, clusterName string, ttl int64) *aerov8.WritePolicy {
	policy := aerospike.GetClusterDefaultWritePolicy(clusterName, ttl)
	applyAerospikeDeadline(ctx, &policy.BasePolicy)
	return policy
}

func applyAerospikeDeadline(ctx context.Context, policy *aerov8.BasePolicy) {
	if policy == nil {
		return
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		return
	}

	timeout := time.Until(deadline)
	if timeout <= 0 {
		timeout = time.Nanosecond
	}

	policy.TotalTimeout = timeout
	if policy.SocketTimeout == 0 || policy.SocketTimeout > timeout {
		policy.SocketTimeout = timeout
	}
}

// Exists checks if a given key exists in the Aerospike store using the provided ExistsRequest.
// Returns true if the key exists, otherwise false with an error if any occurs.
func (a *AerospikeCache) Exists(ctx context.Context, request *ExistsRequest) (bool, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return false, err
	}

	verifiedRequest, err := a.verifyRequest(request)
	if err != nil {
		return false, err
	}

	request, _ = verifiedRequest.(*ExistsRequest)

	aerospikeKey, err := aerospikeNewKey(request.Namespace, request.Collection, request.Key)
	if err != nil {
		return false, fmt.Errorf("failed to create aerospike key: %w", err)
	}

	exists, err := a.aero.GetClient().Exists(aerospikeReadPolicyForContext(ctx), aerospikeKey)
	if err != nil {
		return false, fmt.Errorf("aerospike::exists() failed: %w", err)
	}

	return exists, nil
}

// Get retrieves the value for a given key and optional fields from Aerospike using the provided GetRequest.
// Returns the bin map associated with the key, or an error if the retrieval fails.
func (a *AerospikeCache) Get(ctx context.Context, request *GetRequest) (any, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return nil, err
	}

	verifiedRequest, err := a.verifyRequest(request)
	if err != nil {
		return false, err
	}

	request, _ = verifiedRequest.(*GetRequest)

	aerospikeKey, err := aerospikeNewKey(request.Namespace, request.Collection, request.Key)
	if err != nil {
		return false, fmt.Errorf("failed to create aerospike key: %w", err)
	}

	record, err := a.aero.GetClient().Get(aerospikeReadPolicyForContext(ctx), aerospikeKey, request.Fields...)
	if err != nil {
		return nil, fmt.Errorf("aerospike::get() failed: %w", err)
	}

	return map[string]any(record.Bins), nil
}

// Set stores the given fields in Aerospike under the specified key using the SetRequest.
// Returns true on successful set, or false and an error if the operation fails.
func (a *AerospikeCache) Set(ctx context.Context, request *SetRequest) (bool, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return false, err
	}

	verifiedRequest, err := a.verifyRequest(request)
	if err != nil {
		return false, err
	}

	request, _ = verifiedRequest.(*SetRequest)
	if _, err := aerospikeTTL(request.TTL); err != nil {
		return false, err
	}

	aerospikeKey, err := aerospikeNewKey(request.Namespace, request.Collection, request.Key)
	if err != nil {
		return false, fmt.Errorf("failed to create aerospike key: %w", err)
	}

	writePolicy := aerospikeWritePolicyForContext(ctx, request.Namespace, request.TTL)

	err = a.aero.GetClient().Put(writePolicy, aerospikeKey, request.Fields)
	if err != nil {
		return false, fmt.Errorf("aerospike::put() failed: %w", err)
	}

	return true, nil
}

// Delete removes a record from Aerospike for the specified key provided in the DeleteRequest.
// Returns true if the key was deleted, false otherwise along with an error.
func (a *AerospikeCache) Delete(ctx context.Context, request *DeleteRequest) (bool, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return false, err
	}

	verifiedRequest, err := a.verifyRequest(request)
	if err != nil {
		return false, err
	}

	request, _ = verifiedRequest.(*DeleteRequest)

	aerospikeKey, err := aerospikeNewKey(request.Namespace, request.Collection, request.Key)
	if err != nil {
		return false, fmt.Errorf("failed to create aerospike key: %w", err)
	}

	deleted, err := a.aero.GetClient().Delete(aerospikeWritePolicyForContext(ctx, request.Namespace, -1), aerospikeKey)
	if err != nil {
		return false, fmt.Errorf("aerospike::delete() failed: %w", err)
	}

	return deleted, nil
}

// MultiGet retrieves multiple records from Aerospike for a list of keys provided in the MultiGetRequest.
// Returns a map of key to bin map for each found key, or an error if the operation fails.
func (a *AerospikeCache) MultiGet(ctx context.Context, request *MultiGetRequest) (map[string]any, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return nil, err
	}

	verifiedRequest, err := a.verifyRequest(request)
	if err != nil {
		return nil, err
	}

	request, _ = verifiedRequest.(*MultiGetRequest)

	keys, err := aerospikeNewKeys(request.Namespace, request.Collection, request.Keys)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike keys: %w", err)
	}

	records, err := a.aero.GetClient().BatchGet(aerospikeBatchPolicyForContext(ctx), keys)
	if err != nil {
		return nil, fmt.Errorf("aerospike::batchGet() failed: %w", err)
	}

	result := make(map[string]any)
	for i, record := range records {
		if record != nil {
			result[request.Keys[i]] = map[string]any(record.Bins)
		}
	}

	return result, nil
}

// MultiSet sets multiple records in Aerospike using the provided MultiSetRequest.
// Returns a map of key to boolean indicating whether each record was successfully written.
func (a *AerospikeCache) MultiSet(ctx context.Context, request *MultiSetRequest) (map[string]bool, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return nil, err
	}

	verifiedRequest, err := a.verifyRequest(request)
	if err != nil {
		return nil, err
	}

	request, _ = verifiedRequest.(*MultiSetRequest)
	if _, err := aerospikeTTL(request.TTL); err != nil {
		return nil, err
	}
	var responseMap = make(map[string]bool)

	var lastError error
	for k, fieldEntry := range request.FieldsMap {
		key, err := aerospikeNewKey(request.Namespace, request.Collection, k)
		if err != nil {
			return nil, fmt.Errorf("failed to create aerospike key: %w", err)
		}

		err = a.aero.GetClient().Put(aerospikeWritePolicyForContext(ctx, request.Namespace, request.TTL), key, fieldEntry)
		if err == nil {
			responseMap[k] = true
		} else {
			lastError = err
			responseMap[k] = false
		}
	}

	if lastError != nil {
		return responseMap, fmt.Errorf("error in one or more write op, last error: %w", lastError)
	} else {
		return responseMap, nil
	}
}

// MultiDelete deletes multiple records in Aerospike based on the keys in the MultiDeleteRequest.
// Returns a map of key to boolean indicating whether each record was successfully deleted.
func (a *AerospikeCache) MultiDelete(ctx context.Context, request *MultiDeleteRequest) (map[string]bool, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return nil, err
	}

	verifiedRequest, err := a.verifyRequest(request)
	if err != nil {
		return nil, err
	}

	request, _ = verifiedRequest.(*MultiDeleteRequest)
	var responseMap = make(map[string]bool)

	keys, err := aerospikeNewKeys(request.Namespace, request.Collection, request.Keys)
	if err != nil {
		return nil, fmt.Errorf("failed to create aerospike keys: %w", err)
	}

	records, err := a.aero.GetClient().BatchDelete(aerospikeBatchPolicyForContext(ctx), nil, keys)
	if err != nil {
		return nil, err
	}

	for i, record := range records {
		if record != nil {
			responseMap[request.Keys[i]] = (record.ResultCode == types.OK)
		}
	}

	return responseMap, nil
}

// Increment performs atomic addition on one or more bins in a record specified in the IncrementRequest.
// Returns an error if any part of the operation fails.
func (a *AerospikeCache) Increment(ctx context.Context, request *IncrementRequest) error {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return err
	}

	verifiedRequest, err := a.verifyRequest(request)
	if err != nil {
		return err
	}

	request, _ = verifiedRequest.(*IncrementRequest)
	aerospikeKey, err := aerospikeNewKey(request.Namespace, request.Collection, request.Key)

	if err != nil {
		return fmt.Errorf("failed to create aerospike key: %w", err)
	}

	operations := []*aerov8.Operation{}
	for fieldKey, fieldVal := range request.Fields {
		operations = append(operations, aerov8.AddOp(aerospike.NewBin(fieldKey, fieldVal)))
	}

	_, err = a.aero.GetClient().Operate(aerospikeWritePolicyForContext(ctx, request.Namespace, -1), aerospikeKey, operations...)
	if err != nil {
		return fmt.Errorf("aerospike::Add() failed: %w", err)
	}

	return nil
}

// Decrement performs atomic subtraction by using AppendOp with negative values from the DecrementRequest.
// Returns an error if the operation fails.
func (a *AerospikeCache) Decrement(ctx context.Context, request *DecrementRequest) error {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return err
	}

	verifiedRequest, err := a.verifyRequest(request)
	if err != nil {
		return err
	}

	request, _ = verifiedRequest.(*DecrementRequest)
	aerospikeKey, err := aerospikeNewKey(request.Namespace, request.Collection, request.Key)

	if err != nil {
		return fmt.Errorf("failed to create aerospike key: %w", err)
	}

	operations := []*aerov8.Operation{}
	for fieldKey, fieldVal := range request.Fields {
		operations = append(operations, aerov8.AddOp(aerospike.NewBin(fieldKey, -1*fieldVal)))
	}

	_, err = a.aero.GetClient().Operate(aerospikeWritePolicyForContext(ctx, request.Namespace, -1), aerospikeKey, operations...)
	if err != nil {
		return fmt.Errorf("aerospike::Add() failed in decrement: %w", err)
	}

	return nil
}

// Append appends the given values to their corresponding bins in a record using the AppendRequest.
// Returns an error if the operation fails.
func (a *AerospikeCache) Append(ctx context.Context, request *AppendRequest) error {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return err
	}

	verifiedRequest, err := a.verifyRequest(request)
	if err != nil {
		return err
	}

	request, _ = verifiedRequest.(*AppendRequest)
	aerospikeKey, err := aerospikeNewKey(request.Namespace, request.Collection, request.Key)

	if err != nil {
		return fmt.Errorf("failed to create aerospike key: %w", err)
	}

	operations := []*aerov8.Operation{}
	for fieldKey, fieldVal := range request.Fields {
		operations = append(operations, aerov8.AppendOp(aerospike.NewBin(fieldKey, fieldVal)))
	}

	_, err = a.aero.GetClient().Operate(aerospikeWritePolicyForContext(ctx, request.Namespace, -1), aerospikeKey, operations...)
	if err != nil {
		return fmt.Errorf("aerospike::Append(): %w", err)
	}

	return nil
}

// GetTTL retrieves the TTL (time to live) of a record for a given key using the GetTTLRequest.
// Returns the TTL in seconds or an error if the retrieval fails.
func (a *AerospikeCache) GetTTL(ctx context.Context, request *GetTTLRequest) (int64, error) {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return 0, err
	}

	verifiedRequest, err := a.verifyRequest(request)
	if err != nil {
		return 0, err
	}

	request, _ = verifiedRequest.(*GetTTLRequest)

	aerospikeKey, err := aerospikeNewKey(request.Namespace, request.Collection, request.Key)
	if err != nil {
		return 0, fmt.Errorf("failed to create aerospike key: %w", err)
	}

	record, err := a.aero.GetClient().GetHeader(aerospikeReadPolicyForContext(ctx), aerospikeKey)
	if err != nil {
		return 0, fmt.Errorf("aerospike::getHeader() failed: %w", err)
	}

	return int64(record.Expiration), nil
}

// SetTTL updates the TTL for a given key using the SetTTLRequest.
// Returns an error if the TTL update operation fails.
func (a *AerospikeCache) SetTTL(ctx context.Context, request *SetTTLRequest) error {
	ctx, err := checkedCacheContext(ctx)
	if err != nil {
		return err
	}

	verifiedRequest, err := a.verifyRequest(request)
	if err != nil {
		return err
	}

	request, _ = verifiedRequest.(*SetTTLRequest)
	expiration, err := aerospikeTTL(request.TTL)
	if err != nil {
		return err
	}

	aerospikeKey, err := aerospikeNewKey(request.Namespace, request.Collection, request.Key)
	if err != nil {
		return fmt.Errorf("failed to create aerospike key: %w", err)
	}

	policy := &aerov8.WritePolicy{
		BasePolicy: *aerov8.NewPolicy(),
		Expiration: expiration,
	}
	applyAerospikeDeadline(ctx, &policy.BasePolicy)

	err = a.aero.GetClient().Touch(policy, aerospikeKey)
	if err != nil {
		return fmt.Errorf("aerospike::touch() failed: %w", err)
	}

	return nil
}

// verifyRequest validates and enriches the provided CacheRequest with default namespace and collection.
// Returns the verified request or an error if required fields are missing.
func (a *AerospikeCache) verifyRequest(request CacheRequest) (CacheRequest, error) {
	if request == nil {
		return request, errors.New("error: input cache request struct is nil")
	}

	if request.GetNamespace() == "" {
		request.SetNamespace(a.namespace)

		if request.GetNamespace() == "" {
			return nil, errors.New("error: namespace field is empty")
		}
	}

	if request.GetCollection() == "" {
		request.SetCollection(a.collection)

		if request.GetCollection() == "" {
			return nil, errors.New("error: collection field is empty")
		}
	}

	return request, nil
}

func aerospikeTTL(ttl int64) (uint32, error) {
	if ttl < 0 {
		return aerospike.DefaultRecordTTL, nil
	}
	if ttl > math.MaxUint32 {
		return 0, fmt.Errorf("aerospike: TTL out of range")
	}
	return uint32(ttl), nil // #nosec G115 -- range checked above.
}
