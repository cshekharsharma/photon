package mongo

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type MongoConnector interface {
	Connect(context.Context, *options.ClientOptions) (Client, error)
}

// MongoDbConnector provides a struct implementation for the MongoDbConnectorInterface.
// It contains a DB field which represents the active MongoDB connection.
type MongoDbConnector struct {
	DB Client
}

type Database interface {
	Collection(string, ...options.Lister[options.CollectionOptions]) Collection
}

type Collection interface {
	FindOne(context.Context, interface{}, ...options.Lister[options.FindOneOptions]) SingleResult
	InsertOne(context.Context, interface{}, ...options.Lister[options.InsertOneOptions]) (interface{}, error)
	InsertMany(context.Context, []interface{}, ...options.Lister[options.InsertManyOptions]) ([]interface{}, error)
	DeleteOne(context.Context, interface{}, ...options.Lister[options.DeleteOneOptions]) (int64, error)
	DeleteMany(context.Context, interface{}, ...options.Lister[options.DeleteManyOptions]) (int64, error)
	Find(context.Context, interface{}, ...options.Lister[options.FindOptions]) (Cursor, error)
	CountDocuments(context.Context, interface{}, ...options.Lister[options.CountOptions]) (int64, error)
	Aggregate(context.Context, interface{}, ...options.Lister[options.AggregateOptions]) (Cursor, error)
	UpdateOne(context.Context, interface{}, interface{}, ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, error)
	UpdateMany(context.Context, interface{}, interface{}, ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, error)
	FindOneAndUpdate(context.Context, interface{}, interface{}, ...options.Lister[options.FindOneAndUpdateOptions]) SingleResult
	BulkWrite(context.Context, []mongo.WriteModel, ...options.Lister[options.BulkWriteOptions]) (*mongo.BulkWriteResult, error)
}

type SingleResult interface {
	Decode(interface{}) error
}

type Cursor interface {
	Close(context.Context) error
	Next(context.Context) bool
	Decode(interface{}) error
	All(context.Context, interface{}) error
	Err() error
}

type Client interface {
	Database(string, ...options.Lister[options.DatabaseOptions]) Database
	Disconnect(context.Context) error
	StartSession() (*mongo.Session, error)
	UseSession(ctx context.Context, fn func(context.Context) error) error
	Ping(context.Context) error
}

type mongoClient struct {
	cl *mongo.Client
}

type mongoDatabase struct {
	db *mongo.Database
}

type mongoCollectionClient interface {
	FindOne(context.Context, interface{}, ...options.Lister[options.FindOneOptions]) *mongo.SingleResult
	UpdateOne(context.Context, interface{}, interface{}, ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, error)
	InsertOne(context.Context, interface{}, ...options.Lister[options.InsertOneOptions]) (*mongo.InsertOneResult, error)
	InsertMany(context.Context, any, ...options.Lister[options.InsertManyOptions]) (*mongo.InsertManyResult, error)
	DeleteOne(context.Context, interface{}, ...options.Lister[options.DeleteOneOptions]) (*mongo.DeleteResult, error)
	DeleteMany(context.Context, interface{}, ...options.Lister[options.DeleteManyOptions]) (*mongo.DeleteResult, error)
	Find(context.Context, interface{}, ...options.Lister[options.FindOptions]) (*mongo.Cursor, error)
	Aggregate(context.Context, interface{}, ...options.Lister[options.AggregateOptions]) (*mongo.Cursor, error)
	UpdateMany(context.Context, interface{}, interface{}, ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, error)
	FindOneAndUpdate(context.Context, interface{}, interface{}, ...options.Lister[options.FindOneAndUpdateOptions]) *mongo.SingleResult
	BulkWrite(context.Context, []mongo.WriteModel, ...options.Lister[options.BulkWriteOptions]) (*mongo.BulkWriteResult, error)
	CountDocuments(context.Context, interface{}, ...options.Lister[options.CountOptions]) (int64, error)
}

type mongoCollection struct {
	coll mongoCollectionClient
}

type mongoSingleResult struct {
	sr *mongo.SingleResult
}

type mongoCursor struct {
	mc *mongo.Cursor
}

func (mdc *MongoDbConnector) Connect(ctx context.Context, clientOpts *options.ClientOptions) (Client, error) {
	client, err := mongo.Connect(clientOpts)
	mdc.DB = &mongoClient{cl: client}

	return mdc.DB, err
}

func (mc *mongoClient) Ping(ctx context.Context) error {
	return mc.cl.Ping(ctx, readpref.Primary())
}

func (mc *mongoClient) Database(dbName string, opts ...options.Lister[options.DatabaseOptions]) Database {
	db := mc.cl.Database(dbName, opts...)
	return &mongoDatabase{db: db}
}

func (mc *mongoClient) UseSession(ctx context.Context, fn func(context.Context) error) error {
	return mc.cl.UseSession(ctx, fn)
}

func (mc *mongoClient) StartSession() (*mongo.Session, error) {
	return mc.cl.StartSession()
}

func (mc *mongoClient) Disconnect(ctx context.Context) error {
	return mc.cl.Disconnect(ctx)
}

func (md *mongoDatabase) Collection(colName string, opts ...options.Lister[options.CollectionOptions]) Collection {
	collection := md.db.Collection(colName, opts...)
	return &mongoCollection{coll: collection}
}

func (md *mongoDatabase) Client() Client {
	client := md.db.Client()
	return &mongoClient{cl: client}
}

func (mc *mongoCollection) FindOne(ctx context.Context, filter interface{}, opts ...options.Lister[options.FindOneOptions]) SingleResult {
	singleResult := mc.coll.FindOne(ctx, filter, opts...)
	return &mongoSingleResult{sr: singleResult}
}

func (mc *mongoCollection) UpdateOne(ctx context.Context, filter interface{}, update interface{}, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, error) {
	return mc.coll.UpdateOne(ctx, filter, update, opts...)
}

func (mc *mongoCollection) InsertOne(ctx context.Context, document interface{}, opts ...options.Lister[options.InsertOneOptions]) (interface{}, error) {
	id, err := mc.coll.InsertOne(ctx, document, opts...)
	if id == nil {
		return nil, err
	}
	return id.InsertedID, err
}

func (mc *mongoCollection) InsertMany(ctx context.Context, document []interface{}, opts ...options.Lister[options.InsertManyOptions]) ([]interface{}, error) {
	res, err := mc.coll.InsertMany(ctx, document, opts...)
	if res == nil {
		return nil, err
	}
	return res.InsertedIDs, err
}

func (mc *mongoCollection) DeleteOne(ctx context.Context, filter interface{}, opts ...options.Lister[options.DeleteOneOptions]) (int64, error) {
	count, err := mc.coll.DeleteOne(ctx, filter, opts...)
	if count == nil {
		return 0, err
	}
	return count.DeletedCount, err
}

func (mc *mongoCollection) DeleteMany(ctx context.Context, filter interface{}, opts ...options.Lister[options.DeleteManyOptions]) (int64, error) {
	count, err := mc.coll.DeleteMany(ctx, filter, opts...)
	if count == nil {
		return 0, err
	}
	return count.DeletedCount, err
}

func (mc *mongoCollection) Find(ctx context.Context, filter interface{}, opts ...options.Lister[options.FindOptions]) (Cursor, error) {
	findResult, err := mc.coll.Find(ctx, filter, opts...)
	return &mongoCursor{mc: findResult}, err
}

func (mc *mongoCollection) Aggregate(ctx context.Context, pipeline interface{}, opts ...options.Lister[options.AggregateOptions]) (Cursor, error) {
	aggregateResult, err := mc.coll.Aggregate(ctx, pipeline, opts...)
	return &mongoCursor{mc: aggregateResult}, err
}

func (mc *mongoCollection) UpdateMany(ctx context.Context, filter interface{}, update interface{}, opts ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, error) {
	return mc.coll.UpdateMany(ctx, filter, update, opts...)
}

func (mc *mongoCollection) FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}, opts ...options.Lister[options.FindOneAndUpdateOptions]) SingleResult {
	singleResult := mc.coll.FindOneAndUpdate(ctx, filter, update, opts...)
	return &mongoSingleResult{sr: singleResult}
}

func (mc *mongoCollection) BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...options.Lister[options.BulkWriteOptions]) (*mongo.BulkWriteResult, error) {
	return mc.coll.BulkWrite(ctx, models, opts...)
}

func (mc *mongoCollection) CountDocuments(ctx context.Context, filter interface{}, opts ...options.Lister[options.CountOptions]) (int64, error) {
	return mc.coll.CountDocuments(ctx, filter, opts...)
}

func (sr *mongoSingleResult) Decode(v interface{}) error {
	return sr.sr.Decode(v)
}

func (mr *mongoCursor) Close(ctx context.Context) error {
	return mr.mc.Close(ctx)
}

func (mr *mongoCursor) Next(ctx context.Context) bool {
	return mr.mc.Next(ctx)
}

func (mr *mongoCursor) Decode(v interface{}) error {
	return mr.mc.Decode(v)
}

func (mr *mongoCursor) All(ctx context.Context, result interface{}) error {
	return mr.mc.All(ctx, result)
}

func (mr *mongoCursor) Err() error {
	return mr.mc.Err()
}

var (

	// mutex is used to ensure thread-safe operations when managing MongoDB instances
	// in the instances map.
	mutex sync.RWMutex

	// instances holds references to MongoDB interfaces mapped by a cluster name.
	// This is intended to prevent redundant connections to the same cluster.
	instances map[string]Client

	// mongodb connection configuration
	connectionConfigMap map[string]*ConnectionConfig

	connectBeforeWriteLockHook = func(clusterName string) {}
)

func SetConnectionConfig(clusterName string, config *ConnectionConfig) {
	mutex.Lock()
	defer mutex.Unlock()

	if connectionConfigMap == nil {
		connectionConfigMap = make(map[string]*ConnectionConfig)
	}

	connectionConfigMap[clusterName] = cloneMongoConnectionConfig(config)
}

func SetConnectionConfigE(clusterName string, config *ConnectionConfig) error {
	clusterName = strings.TrimSpace(clusterName)
	if clusterName == "" {
		return fmt.Errorf("mongo: cluster name is required")
	}
	if err := config.Validate(); err != nil {
		return err
	}

	SetConnectionConfig(clusterName, config)
	return nil
}

func Connect(ctx context.Context, connector MongoConnector, clusterName string) (Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if connector == nil {
		return nil, fmt.Errorf("mongo: connector is required")
	}

	mutex.RLock()
	if instances != nil {
		if db := instances[clusterName]; db != nil {
			mutex.RUnlock()
			return db, nil
		}
	}
	mutex.RUnlock()

	connectBeforeWriteLockHook(clusterName)
	mutex.Lock()
	defer mutex.Unlock()

	if instances == nil {
		instances = make(map[string]Client)
	}
	if db := instances[clusterName]; db != nil {
		return db, nil
	}

	cfg, err := getConnectionConfigLocked(clusterName)
	if err != nil {
		return nil, err
	}
	newConn, err := newInstance(ctx, connector, cfg)
	if err != nil {
		return nil, err
	}

	instances[clusterName] = newConn
	return newConn, nil
}

// newInstance establishes a new MongoDB client from a validated connection config.
func newInstance(ctx context.Context, connector MongoConnector, cfg *ConnectionConfig) (Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if connector == nil {
		return nil, fmt.Errorf("mongo: connector is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg = cfg.normalized()

	clientOpts := options.Client()

	//clientOpts.SetServerAPIOptions(options.ServerAPI(options.ServerAPIVersion1))
	clientOpts.ApplyURI(buildConnectionURI(cfg))
	clientOpts.SetRetryWrites(true)
	clientOpts.SetWriteConcern(writeconcern.Majority())
	clientOpts.SetConnectTimeout(cfg.ConnectionTimeout)
	clientOpts.SetMaxPoolSize(uint64(cfg.ConnectionPoolsize)) // #nosec G115 -- config validation rejects negatives.

	// Create a new client and connect to the server
	return connector.Connect(ctx, clientOpts)
}

// Get mongodb connection string from configuratioh for provided cluster name
func getConnectionUri(clusterName string) string {
	mutex.RLock()
	defer mutex.RUnlock()

	if config, ok := connectionConfigMap[clusterName]; ok && config != nil {
		if err := config.Validate(); err == nil {
			return buildConnectionURI(config.normalized())
		}
	}

	// returning default connection string without auth,
	// and assumed to be running on localhost with default port.
	return "mongodb://localhost:27017/?"
}

// Get maximum connection poolsize configuration for provided cluster name
func getMaxConnectionPoolSize(clusterName string) uint64 {
	mutex.RLock()
	defer mutex.RUnlock()

	if config, ok := connectionConfigMap[clusterName]; ok && config != nil {
		if config.ConnectionPoolsize > 0 {
			return uint64(config.ConnectionPoolsize) // #nosec G115 -- positive value checked above.
		}
	}
	return DefaultConnectionPoolSize
}

// Get connection time out configuration for provided cluster name
func getConnectionTimeout(clusterName string) time.Duration {
	mutex.RLock()
	defer mutex.RUnlock()

	if config, ok := connectionConfigMap[clusterName]; ok && config != nil && config.ConnectionTimeout > 0 {
		return config.ConnectionTimeout
	}
	return DefaultConnectionTimeout
}

func getConnectionConfigLocked(clusterName string) (*ConnectionConfig, error) {
	if config, ok := connectionConfigMap[clusterName]; ok && config != nil {
		if err := config.Validate(); err != nil {
			return nil, err
		}
		return config.normalized(), nil
	}

	return (&ConnectionConfig{
		Hosts:              []string{"localhost:27017"},
		ConnectionTimeout:  DefaultConnectionTimeout,
		ConnectionPoolsize: int64(DefaultConnectionPoolSize),
	}).normalized(), nil
}

func buildConnectionURI(config *ConnectionConfig) string {
	u := &url.URL{
		Scheme: "mongodb",
		Host:   strings.Join(config.Hosts, ","),
		Path:   "/",
	}
	if strings.TrimSpace(config.Username) != "" || strings.TrimSpace(config.Password) != "" {
		u.User = url.UserPassword(config.Username, config.Password)
	}
	u.RawQuery = ""
	return u.String() + "?"
}

func cloneMongoConnectionConfig(config *ConnectionConfig) *ConnectionConfig {
	if config == nil {
		return nil
	}
	clone := *config
	clone.Hosts = append([]string(nil), config.Hosts...)
	return &clone
}
