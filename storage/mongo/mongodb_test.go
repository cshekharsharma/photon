package mongo

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type mockClient struct{}

type fakeMongoConnector struct {
	calls int
	err   error
}

func (f *fakeMongoConnector) Connect(ctx context.Context, clientOpts *options.ClientOptions) (Client, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &mockClient{}, nil
}

func (mc *mockClient) Database(dbName string, opts ...options.Lister[options.DatabaseOptions]) Database {
	return nil
}

func (mc *mockClient) Disconnect(ctx context.Context) error {
	return nil
}

func (mc *mockClient) UseSession(ctx context.Context, fn func(context.Context) error) error {
	return nil
}

func (mc *mockClient) StartSession() (*mongo.Session, error) {
	return nil, nil
}

func (mc *mockClient) Ping(ctx context.Context) error {
	return nil
}

func resetMongoGlobals() {
	mutex.Lock()
	defer mutex.Unlock()
	instances = nil
	connectionConfigMap = nil
	connectBeforeWriteLockHook = func(clusterName string) {}
}

func TestConnect(t *testing.T) {
	t.Parallel()

	SetConnectionConfig("dummy", &ConnectionConfig{
		Hosts:              []string{"127.0.0.1:27017"},
		Username:           "d",
		Password:           "d",
		ConnectionTimeout:  100,
		ConnectionPoolsize: 100,
	})

	_, err := Connect(context.Background(), &MongoDbConnector{}, "dummy")
	assert.Nil(t, err)
}

func TestMongoDbConnector_Connect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		connector     MongoConnector
		clientOptions *options.ClientOptions
		expectedError error
	}{
		{
			name: "SuccessfulConnection",
			connector: &MongoDbConnector{
				DB: &mockClient{},
			},
			clientOptions: options.Client(),
			expectedError: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.connector.Connect(context.Background(), tc.clientOptions)
			assert.Equal(t, tc.expectedError, err)
		})
	}
}

func Test_getConnectionUri(t *testing.T) {
	resetMongoGlobals()
	cluster := "cluster1"
	SetConnectionConfig(cluster, &ConnectionConfig{
		Hosts:              []string{"host1", "host2"},
		Username:           "user",
		Password:           "pass",
		ConnectionTimeout:  7,
		ConnectionPoolsize: 100,
	})

	t.Run("ReturnsAuthUriForConfiguredCluster", func(t *testing.T) {
		uri := getConnectionUri(cluster)
		expected := "mongodb://user:pass@host1,host2/?"

		if uri != expected {
			t.Errorf("expected %s, got %s", expected, uri)
		}
	})

	t.Run("ReturnsDefaultUriForUnknownCluster", func(t *testing.T) {
		uri := getConnectionUri("unknown")
		if uri != "mongodb://localhost:27017/?" {
			t.Errorf("expected default uri, got %s", uri)
		}
	})
}

func TestConnectionConfigValidationAndEscapedURI(t *testing.T) {
	resetMongoGlobals()

	cfg := &ConnectionConfig{
		Hosts:             []string{"mongo-1:27017", "mongo-2:27017"},
		Username:          "user@example.com",
		Password:          "p/a?ss",
		ConnectionTimeout: 10 * time.Second,
	}
	if err := SetConnectionConfigE("escaped", cfg); err != nil {
		t.Fatalf("set config: %v", err)
	}

	got := getConnectionUri("escaped")
	want := "mongodb://user%40example.com:p%2Fa%3Fss@mongo-1:27017,mongo-2:27017/?"
	if got != want {
		t.Fatalf("unexpected uri: got %q want %q", got, want)
	}

	if err := SetConnectionConfigE("", cfg); err == nil {
		t.Fatal("expected empty cluster error")
	}
	if err := SetConnectionConfigE("bad", &ConnectionConfig{Hosts: []string{"localhost"}, ConnectionPoolsize: -1}); err == nil {
		t.Fatal("expected negative pool error")
	}
	if err := SetConnectionConfigE("bad-timeout", &ConnectionConfig{Hosts: []string{"localhost"}, ConnectionTimeout: -1}); err == nil {
		t.Fatal("expected negative timeout error")
	}
}

func Test_getMaxConnectionPoolSize(t *testing.T) {
	resetMongoGlobals()
	cluster := "cluster2"
	SetConnectionConfig(cluster, &ConnectionConfig{
		Hosts:              []string{"localhost"},
		Username:           "a",
		Password:           "b",
		ConnectionTimeout:  7,
		ConnectionPoolsize: 42,
	})

	t.Run("ReturnsConfiguredPoolSize", func(t *testing.T) {
		got := getMaxConnectionPoolSize(cluster)
		if got != 42 {
			t.Errorf("expected pool size 42, got %d", got)
		}
	})

	t.Run("ReturnsDefaultPoolSizeForUnknownCluster", func(t *testing.T) {
		got := getMaxConnectionPoolSize("unknown")
		if got != DefaultConnectionPoolSize {
			t.Errorf("expected default pool size %d, got %d", DefaultConnectionPoolSize, got)
		}
	})
}

func TestMongoConfigZeroValuesUseDefaults(t *testing.T) {
	resetMongoGlobals()

	SetConnectionConfig("zero-values", &ConnectionConfig{Hosts: []string{"localhost:27017"}})
	if got := getMaxConnectionPoolSize("zero-values"); got != DefaultConnectionPoolSize {
		t.Fatalf("expected default pool size, got %d", got)
	}
	if got := getConnectionTimeout("zero-values"); got != DefaultConnectionTimeout {
		t.Fatalf("expected default timeout, got %s", got)
	}
}

func Test_getConnectionTimeout(t *testing.T) {
	resetMongoGlobals()
	cluster := "cluster3"
	SetConnectionConfig(cluster, &ConnectionConfig{
		Hosts:              []string{"localhost"},
		Username:           "a",
		Password:           "b",
		ConnectionTimeout:  7 * time.Second,
		ConnectionPoolsize: 100,
	})

	t.Run("ReturnsConfiguredTimeout", func(t *testing.T) {
		got := getConnectionTimeout(cluster)
		if got != 7*time.Second {
			t.Errorf("expected timeout 7s, got %v", got)
		}
	})

	t.Run("ReturnsDefaultTimeoutForUnknownCluster", func(t *testing.T) {
		got := getConnectionTimeout("unknown")
		if got != DefaultConnectionTimeout {
			t.Errorf("expected default timeout %v, got %v", DefaultConnectionTimeout, got)
		}
	})
}

func TestConnectRejectsInvalidConfigAndNilConnector(t *testing.T) {
	resetMongoGlobals()

	if _, err := Connect(context.Background(), nil, "cluster"); err == nil {
		t.Fatal("expected nil connector error")
	}

	SetConnectionConfig("bad", &ConnectionConfig{Hosts: []string{"localhost"}, ConnectionPoolsize: -1})
	if _, err := Connect(context.Background(), &MongoDbConnector{}, "bad"); err == nil {
		t.Fatal("expected invalid config error")
	}
}

func TestConnectEdges(t *testing.T) {
	resetMongoGlobals()

	connector := &fakeMongoConnector{}
	var nilCtx context.Context
	client, err := Connect(nilCtx, connector, "default")
	require.NoError(t, err)
	require.NotNil(t, client)

	again, err := Connect(context.Background(), connector, "default")
	require.NoError(t, err)
	assert.Same(t, client, again)
	assert.Equal(t, 1, connector.calls)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	client, err = Connect(cancelled, connector, "cancelled")
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestConnectUsesSingletonAndNewInstanceHelpers(t *testing.T) {
	resetMongoGlobals()

	SetConnectionConfig("singleton", &ConnectionConfig{Hosts: []string{"localhost:27017"}})
	connector := &fakeMongoConnector{}

	first, err := Connect(context.Background(), connector, "singleton")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	second, err := Connect(context.Background(), connector, "singleton")
	if err != nil {
		t.Fatalf("connect again: %v", err)
	}
	if first != second {
		t.Fatal("expected singleton client")
	}
	if connector.calls != 1 {
		t.Fatalf("expected one connect call, got %d", connector.calls)
	}

	if client, err := newInstance(context.Background(), &fakeMongoConnector{}, &ConnectionConfig{Hosts: []string{"localhost:27017"}}); err != nil || client == nil {
		t.Fatalf("expected default newInstance success, client=%#v err=%v", client, err)
	}
}

func TestConnectDoubleCheckAndNewInstanceErrors(t *testing.T) {
	resetMongoGlobals()

	existing := &mockClient{}
	connectBeforeWriteLockHook = func(clusterName string) {
		if instances == nil {
			instances = make(map[string]Client)
		}
		instances[clusterName] = existing
	}
	got, err := Connect(context.Background(), &fakeMongoConnector{}, "double-check")
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if got != existing {
		t.Fatal("expected existing instance from double-check branch")
	}

	resetMongoGlobals()
	SetConnectionConfig("connect-error", &ConnectionConfig{Hosts: []string{"localhost:27017"}})
	if _, err := Connect(context.Background(), &fakeMongoConnector{err: errors.New("connect fail")}, "connect-error"); err == nil {
		t.Fatal("expected connect error")
	}

	if _, err := newInstance(context.Background(), &fakeMongoConnector{}, &ConnectionConfig{Hosts: []string{"localhost:27017"}, ConnectionPoolsize: -1}); err == nil {
		t.Fatal("expected invalid config error")
	}
	if _, err := newInstance(context.Background(), nil, &ConnectionConfig{Hosts: []string{"localhost:27017"}}); err == nil {
		t.Fatal("expected nil connector error")
	}
	if _, err := newInstance(context.Background(), &fakeMongoConnector{}, &ConnectionConfig{Hosts: []string{"localhost:27017"}, ConnectionTimeout: -1}); err == nil {
		t.Fatal("expected invalid config error")
	}
}

func TestNewInstanceEdges(t *testing.T) {
	resetMongoGlobals()

	var nilCtx context.Context
	client, err := newInstance(nilCtx, &fakeMongoConnector{}, &ConnectionConfig{Hosts: []string{"localhost:27017"}})
	require.NoError(t, err)
	require.NotNil(t, client)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	client, err = newInstance(cancelled, &fakeMongoConnector{}, &ConnectionConfig{Hosts: []string{"localhost:27017"}})
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestMongoConnectionConfigValidationBranches(t *testing.T) {
	var cfg *ConnectionConfig
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected nil config error")
	}
	if err := (&ConnectionConfig{}).Validate(); err == nil {
		t.Fatal("expected missing host error")
	}
	if err := (&ConnectionConfig{Hosts: []string{" "}}).Validate(); err == nil {
		t.Fatal("expected empty host error")
	}
	if normalized := cfg.normalized(); normalized != nil {
		t.Fatalf("expected nil normalized config, got %#v", normalized)
	}
	if clone := cloneMongoConnectionConfig(nil); clone != nil {
		t.Fatalf("expected nil clone, got %#v", clone)
	}
}

func TestMongoConfigConcurrentAccess(t *testing.T) {
	resetMongoGlobals()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			SetConnectionConfig("concurrent", &ConnectionConfig{Hosts: []string{"localhost:27017"}})
		}()
		go func() {
			defer wg.Done()
			_ = getConnectionUri("concurrent")
			_ = getMaxConnectionPoolSize("concurrent")
			_ = getConnectionTimeout("concurrent")
		}()
	}
	wg.Wait()
}

func TestMongoClient_Ping(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:1"))
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	mc := &mongoClient{cl: client}

	err = mc.Ping(ctx)

	if err == nil {
		t.Fatalf("expected Ping to fail with unreachable server")
	}
}

func TestMongoClient_Database_ReturnsWrappedDatabase(t *testing.T) {
	t.Parallel()

	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:1"))
	if err == nil {
		defer func() {
			require.NoError(t, client.Disconnect(context.Background()))
		}()
	}

	mc := &mongoClient{cl: client}

	db := mc.Database("mydb")
	if db == nil {
		t.Fatalf("expected non-nil Database wrapper")
	}

	if _, ok := db.(*mongoDatabase); !ok {
		t.Fatalf("expected *mongoDatabase, got %T", db)
	}
}

func TestMongoClient_Disconnect_NoPanic(t *testing.T) {
	t.Parallel()

	cl, err := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:1"))
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	mc := &mongoClient{cl: cl}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_ = mc.Disconnect(ctx)
}

func TestMongoDatabase_Client(t *testing.T) {
	t.Parallel()

	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://127.0.0.1:1"))
	if err == nil {
		defer func() {
			require.NoError(t, client.Disconnect(context.Background()))
		}()
	}

	db := client.Database("testdb")

	md := &mongoDatabase{db: db}

	cl := md.Client()
	if cl == nil {
		t.Fatalf("expected non-nil Client")
	}

	if _, ok := cl.(*mongoClient); !ok {
		t.Fatalf("expected *mongoClient, got %T", cl)
	}
}

func TestMongoSingleResult_Decode(t *testing.T) {
	t.Parallel()

	doc := bson.M{"name": "Alice", "age": 30}

	sr := mongo.NewSingleResultFromDocument(doc, nil, nil)
	wrapped := &mongoSingleResult{sr: sr}

	var result bson.M
	if err := wrapped.Decode(&result); err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}

	expected := bson.M{"name": "Alice", "age": int32(30)}
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("decoded value mismatch, expected %+v, got %+v", expected, result)
	}
}

func TestMongoCursor_Close(t *testing.T) {
	t.Parallel()

	cur, err := mongo.NewCursorFromDocuments([]interface{}{
		bson.M{"x": 1},
	}, nil, nil)
	if err != nil {
		t.Fatalf("failed to create cursor: %v", err)
	}

	mc := &mongoCursor{mc: cur}

	if err := mc.Close(context.Background()); err != nil {
		t.Fatalf("expected nil error on Close, got %v", err)
	}

	if err := mc.Close(context.Background()); err != nil {
		t.Fatalf("expected nil error on second Close, got %v", err)
	}
}

func TestMongoCursor_Decode(t *testing.T) {
	t.Parallel()

	docs := []interface{}{
		bson.M{"name": "Alice", "age": 30},
	}

	cur, err := mongo.NewCursorFromDocuments(docs, nil, nil)
	if err != nil {
		t.Fatalf("failed to create cursor: %v", err)
	}

	mc := &mongoCursor{mc: cur}

	if !mc.Next(context.Background()) {
		t.Fatalf("expected cursor to have one document")
	}

	var result bson.M
	if err := mc.Decode(&result); err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}

	expected := bson.M{"name": "Alice", "age": int32(30)}
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("decoded value mismatch, expected %+v, got %+v", expected, result)
	}
}

func TestMongoCursor_All(t *testing.T) {
	t.Parallel()

	docs := []interface{}{
		bson.M{"name": "Alice", "age": 30},
		bson.M{"name": "Bob", "age": 25},
	}

	cur, err := mongo.NewCursorFromDocuments(docs, nil, nil)
	if err != nil {
		t.Fatalf("failed to create cursor: %v", err)
	}

	mc := &mongoCursor{mc: cur}

	var results []bson.M
	if err := mc.All(context.Background(), &results); err != nil {
		t.Fatalf("All returned error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 documents, got %d", len(results))
	}

	if results[0]["name"] != "Alice" || results[1]["name"] != "Bob" {
		t.Fatalf("unexpected results: %+v", results)
	}
}

func TestMongoCursor_Err(t *testing.T) {
	t.Parallel()

	cur, err := mongo.NewCursorFromDocuments([]interface{}{}, nil, nil)
	if err != nil {
		t.Fatalf("failed to create cursor: %v", err)
	}

	mc := &mongoCursor{mc: cur}

	if err := mc.Err(); err != nil {
		t.Fatalf("expected nil error on empty cursor, got %v", err)
	}

	if mc.Next(context.Background()) {
		var m bson.M
		_ = mc.Decode(&m)
	}

	if err := mc.Err(); err != nil {
		t.Fatalf("expected nil error after iteration, got %v", err)
	}
}

func newTestMongoClient(t *testing.T) *mongo.Client {
	t.Helper()
	client, err := mongo.Connect(options.Client().
		ApplyURI("mongodb://localhost:27017").
		SetServerSelectionTimeout(10 * time.Millisecond))

	require.NoError(t, err)
	return client
}

type fakeMongoCollectionClient struct{}

func (f *fakeMongoCollectionClient) FindOne(ctx context.Context, filter interface{}, opts ...options.Lister[options.FindOneOptions]) *mongo.SingleResult {
	return mongo.NewSingleResultFromDocument(bson.M{"ok": true}, nil, nil)
}

func (f *fakeMongoCollectionClient) UpdateOne(ctx context.Context, filter interface{}, update interface{}, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, error) {
	return &mongo.UpdateResult{MatchedCount: 1, ModifiedCount: 1}, nil
}

func (f *fakeMongoCollectionClient) InsertOne(ctx context.Context, document interface{}, opts ...options.Lister[options.InsertOneOptions]) (*mongo.InsertOneResult, error) {
	return &mongo.InsertOneResult{InsertedID: "id-1"}, nil
}

func (f *fakeMongoCollectionClient) InsertMany(ctx context.Context, document any, opts ...options.Lister[options.InsertManyOptions]) (*mongo.InsertManyResult, error) {
	return &mongo.InsertManyResult{InsertedIDs: []interface{}{"id-1", "id-2"}}, nil
}

func (f *fakeMongoCollectionClient) DeleteOne(ctx context.Context, filter interface{}, opts ...options.Lister[options.DeleteOneOptions]) (*mongo.DeleteResult, error) {
	return &mongo.DeleteResult{DeletedCount: 1}, nil
}

func (f *fakeMongoCollectionClient) DeleteMany(ctx context.Context, filter interface{}, opts ...options.Lister[options.DeleteManyOptions]) (*mongo.DeleteResult, error) {
	return &mongo.DeleteResult{DeletedCount: 2}, nil
}

func (f *fakeMongoCollectionClient) Find(ctx context.Context, filter interface{}, opts ...options.Lister[options.FindOptions]) (*mongo.Cursor, error) {
	return mongo.NewCursorFromDocuments([]interface{}{bson.M{"x": 1}}, nil, nil)
}

func (f *fakeMongoCollectionClient) Aggregate(ctx context.Context, pipeline interface{}, opts ...options.Lister[options.AggregateOptions]) (*mongo.Cursor, error) {
	return mongo.NewCursorFromDocuments([]interface{}{bson.M{"x": 1}}, nil, nil)
}

func (f *fakeMongoCollectionClient) UpdateMany(ctx context.Context, filter interface{}, update interface{}, opts ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, error) {
	return &mongo.UpdateResult{MatchedCount: 2, ModifiedCount: 2}, nil
}

func (f *fakeMongoCollectionClient) FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}, opts ...options.Lister[options.FindOneAndUpdateOptions]) *mongo.SingleResult {
	return mongo.NewSingleResultFromDocument(bson.M{"ok": true}, nil, nil)
}

func (f *fakeMongoCollectionClient) BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...options.Lister[options.BulkWriteOptions]) (*mongo.BulkWriteResult, error) {
	return &mongo.BulkWriteResult{InsertedCount: 1}, nil
}

func (f *fakeMongoCollectionClient) CountDocuments(ctx context.Context, filter interface{}, opts ...options.Lister[options.CountOptions]) (int64, error) {
	return 3, nil
}

func TestMongoCollection_WrapperSuccessPaths(t *testing.T) {
	coll := &mongoCollection{coll: &fakeMongoCollectionClient{}}
	ctx := context.Background()

	if id, err := coll.InsertOne(ctx, bson.M{"x": 1}); err != nil || id != "id-1" {
		t.Fatalf("InsertOne mismatch: id=%v err=%v", id, err)
	}
	if ids, err := coll.InsertMany(ctx, []interface{}{bson.M{"x": 1}}); err != nil || len(ids) != 2 {
		t.Fatalf("InsertMany mismatch: ids=%v err=%v", ids, err)
	}
	if deleted, err := coll.DeleteOne(ctx, bson.M{"x": 1}); err != nil || deleted != 1 {
		t.Fatalf("DeleteOne mismatch: deleted=%d err=%v", deleted, err)
	}
	if deleted, err := coll.DeleteMany(ctx, bson.M{"x": 1}); err != nil || deleted != 2 {
		t.Fatalf("DeleteMany mismatch: deleted=%d err=%v", deleted, err)
	}
}

func TestMongoClientAndCollectionMethods(t *testing.T) {
	client := newTestMongoClient(t)
	mc := &mongoClient{cl: client}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	// Client methods
	_ = mc.Ping(ctx)
	_, _ = mc.StartSession()
	_ = mc.UseSession(ctx, func(context.Context) error { return nil })

	db := mc.Database("testdb")
	assert.NotNil(t, db)

	coll := db.Collection("col")
	assert.NotNil(t, coll)

	// Collection methods (expected errors due to no server)
	_ = coll.FindOne(ctx, map[string]interface{}{})
	_, _ = coll.InsertOne(ctx, map[string]interface{}{"x": 1})
	_, _ = coll.InsertMany(ctx, []interface{}{map[string]interface{}{"x": 1}})
	_, _ = coll.DeleteOne(ctx, map[string]interface{}{})
	_, _ = coll.DeleteMany(ctx, map[string]interface{}{})
	_, _ = coll.Find(ctx, map[string]interface{}{})
	_, _ = coll.Aggregate(ctx, []interface{}{})
	_, _ = coll.UpdateOne(ctx, map[string]interface{}{}, map[string]interface{}{"$set": map[string]interface{}{"x": 1}})
	_, _ = coll.UpdateMany(ctx, map[string]interface{}{}, map[string]interface{}{"$set": map[string]interface{}{"x": 1}})
	_ = coll.FindOneAndUpdate(ctx, map[string]interface{}{}, map[string]interface{}{"$set": map[string]interface{}{"x": 1}})
	_, _ = coll.BulkWrite(ctx, []mongo.WriteModel{})
	_, _ = coll.CountDocuments(ctx, map[string]interface{}{})
}
