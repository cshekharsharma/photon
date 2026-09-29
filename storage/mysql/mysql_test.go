package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockedMySqlDbConnector struct {
	mock.Mock
}

func (mdbc *MockedMySqlDbConnector) Open(driverName, dataSourceName string) (MySqlDbInterface, error) {
	args := mdbc.Called(driverName, dataSourceName)
	return args.Get(0).(MySqlDbInterface), args.Error(1)
}

type MockedMySqlDb struct {
	mock.Mock
}

func (mdb *MockedMySqlDb) Ping(ctx context.Context) error {
	args := mdb.Called()
	return args.Error(0)
}

func (mdb *MockedMySqlDb) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	calledArgs := mdb.Called(query, args)
	result, _ := calledArgs.Get(0).(sql.Result)
	return result, calledArgs.Error(1)
}

func (mdb *MockedMySqlDb) Prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	args := mdb.Called(query)
	stmt, _ := args.Get(0).(*sql.Stmt)
	return stmt, args.Error(1)
}

func (mdb *MockedMySqlDb) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	callArgs := mdb.Called(append([]interface{}{query}, args...)...)

	var rows *sql.Rows
	if r := callArgs.Get(0); r != nil {
		rows = r.(*sql.Rows)
	}

	return rows, callArgs.Error(1)
}

func (mdb *MockedMySqlDb) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return &sql.Row{}
}

func (mdb *MockedMySqlDb) SetConnMaxLifetime(d time.Duration) {}

func (mdb *MockedMySqlDb) SetMaxIdleConns(n int) {}

func (mdb *MockedMySqlDb) SetMaxOpenConns(n int) {}

func (mdb *MockedMySqlDb) SetConnMaxIdleTime(d time.Duration) {}

func (mdb *MockedMySqlDb) Stats() sql.DBStats {
	return sql.DBStats{}
}

func (mdb *MockedMySqlDb) Begin() (*sql.Tx, error) {
	return nil, nil
}

func (mdb *MockedMySqlDb) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return nil, nil
}

func (mdb *MockedMySqlDb) Driver() driver.Driver {
	return nil
}

func (mdb *MockedMySqlDb) Conn(ctx context.Context) (*sql.Conn, error) {
	return nil, nil
}

func (mdb *MockedMySqlDb) Close() error {
	return nil
}

type MockedSqlResult struct {
	mock.Mock
}

func (m *MockedSqlResult) LastInsertId() (int64, error) {
	args := m.Called()
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockedSqlResult) RowsAffected() (int64, error) {
	args := m.Called()
	return args.Get(0).(int64), args.Error(1)
}

type MockedSqlStmt struct {
	mock.Mock
}

func (m *MockedSqlStmt) Exec(args ...interface{}) (sql.Result, error) {
	argsList := m.Called(args...)
	return argsList.Get(0).(sql.Result), argsList.Error(1)
}

func (m *MockedSqlStmt) Close() error {
	return m.Called().Error(0)
}

func TestNewInstance(t *testing.T) {
	mockedMyDb := new(MockedMySqlDb)
	mockedMyDb.On("Ping").Return(nil)

	mockedMyDBC := new(MockedMySqlDbConnector)
	mockedMyDBC.On("Open", mock.Anything, mock.Anything).Return(mockedMyDb, nil)

	dbconfig := &ConnectionConfig{
		Host:            "test-host",
		Port:            "3006",
		UserName:        "username",
		Password:        "password",
		DbName:          "dbname",
		MaxOpenConn:     10,
		MaxIdleConn:     10,
		ConnMaxIdleTime: 10,
		MaxConnLifetime: 100,
	}

	db, err := newInstance(context.Background(), mockedMyDBC, dbconfig)

	assert.NoError(t, err)
	assert.Equal(t, mockedMyDb, db)
	assert.NoError(t, db.Ping(context.Background()))
	assert.NoError(t, db.Ping(context.TODO()))
	assert.IsType(t, sql.DBStats{}, db.Stats())

	// Testing Ping failure
	mockedMyDb2 := new(MockedMySqlDb)
	mockedMyDb2.On("Ping").Return(errors.New("Ping failed."))

	mockedMyDBC2 := new(MockedMySqlDbConnector)
	mockedMyDBC2.On("Open", mock.Anything, mock.Anything).Return(mockedMyDb2, nil)

	_, err = newInstance(context.Background(), mockedMyDBC2, dbconfig)

	assert.Error(t, err)
}

func TestNewInstancePingError(t *testing.T) {
	mockedMyDb2 := new(MockedMySqlDb)
	mockedMyDb2.On("Ping").Return(errors.New("Ping failed."))

	mockedMyDBC2 := new(MockedMySqlDbConnector)
	mockedMyDBC2.On("Open", mock.Anything, mock.Anything).Return(mockedMyDb2, nil)

	dbconfig := &ConnectionConfig{
		Host:            "test-host",
		Port:            "3006",
		UserName:        "username",
		Password:        "password",
		DbName:          "dbname",
		MaxOpenConn:     10,
		MaxIdleConn:     10,
		ConnMaxIdleTime: 10,
		MaxConnLifetime: 100,
	}

	_, err := newInstance(context.Background(), mockedMyDBC2, dbconfig)

	assert.Error(t, err)
}

func TestNewInstanceOpenError(t *testing.T) {
	mockedMyDb2 := new(MockedMySqlDb)
	mockedMyDb2.On("Ping").Return(nil)

	mockedMyDBC2 := new(MockedMySqlDbConnector)
	mockedMyDBC2.On("Open", mock.Anything, mock.Anything).Return(mockedMyDb2, errors.New("Open failed"))

	dbconfig := &ConnectionConfig{
		Host:            "test-host",
		Port:            "3006",
		UserName:        "username",
		Password:        "password",
		DbName:          "dbname",
		MaxOpenConn:     10,
		MaxIdleConn:     10,
		ConnMaxIdleTime: 10,
		MaxConnLifetime: 100,
	}

	_, err := newInstance(context.Background(), mockedMyDBC2, dbconfig)

	assert.Error(t, err)
}

/***** more tests can be added here *****/
func TestMySqlDbConnector_Open(t *testing.T) {
	mockConnector := &MySqlDbConnector{}

	db, err := mockConnector.Open("mysql", "user:pass@tcp(localhost:3306)/testdb")
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.Equal(t, mockConnector.DB, db)
}

func TestSetConnectionConfig(t *testing.T) {
	connectionConfigMap = nil
	clusterName := "test-cluster"
	cfg := &ConnectionConfig{
		Host:            "localhost",
		Port:            "3306",
		UserName:        "user",
		Password:        "pass",
		DbName:          "testdb",
		MaxOpenConn:     10,
		MaxIdleConn:     5,
		ConnMaxIdleTime: 30,
	}
	SetConnectionConfig(clusterName, cfg)
	assert.Equal(t, cfg, connectionConfigMap[clusterName])

	cfg.Host = "changed"
	assert.NotEqual(t, cfg.Host, connectionConfigMap[clusterName].Host)
}

func TestConnect(t *testing.T) {
	instances = nil
	connectionConfigMap = nil
	clusterName := "test-cluster-conn"
	cfg := &ConnectionConfig{
		Host:            "localhost",
		Port:            "3306",
		UserName:        "user",
		Password:        "pass",
		DbName:          "testdb",
		MaxOpenConn:     10,
		MaxIdleConn:     5,
		ConnMaxIdleTime: 30,
	}

	SetConnectionConfig(clusterName, cfg)

	mockedDb := new(MockedMySqlDb)
	mockedDb.On("Ping").Return(nil)

	mockedConnector := new(MockedMySqlDbConnector)
	mockedConnector.On("Open", mock.Anything, mock.Anything).Return(mockedDb, nil)

	db, err := Connect(context.Background(), mockedConnector, clusterName)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	db2, err := Connect(context.Background(), mockedConnector, clusterName)
	assert.NoError(t, err)
	assert.Equal(t, db, db2)

	_, err = Connect(context.Background(), mockedConnector, "missing-cluster")
	assert.Error(t, err)
}

func TestConnectAndConfigEdges(t *testing.T) {
	instances = nil
	connectionConfigMap = nil

	connector := new(MockedMySqlDbConnector)
	mockedDb := new(MockedMySqlDb)
	mockedDb.On("Ping").Return(nil)
	connector.On("Open", mock.Anything, mock.Anything).Return(mockedDb, nil)

	_, err := Connect(context.Background(), connector, "missing-map")
	assert.Error(t, err)

	cfg := &ConnectionConfig{Host: "localhost", Port: "3306", UserName: "user", Password: "pass", DbName: "db"}
	SetConnectionConfig("ctx", cfg)

	var nilCtx context.Context
	db, err := Connect(nilCtx, connector, "ctx")
	assert.NoError(t, err)
	assert.Equal(t, mockedDb, db)

	got := GetConnectionConfig("ctx")
	assert.Equal(t, "localhost", got.Host)
	got.Host = "changed"
	assert.Equal(t, "localhost", GetConnectionConfig("ctx").Host)

	assert.Nil(t, cloneConnectionConfig(nil))

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	db, err = Connect(cancelled, connector, "cancelled")
	assert.Error(t, err)
	assert.Nil(t, db)

	instances = nil
	connectionConfigMap = nil
	assert.Nil(t, GetConnectionConfig("missing"))
}

func TestNewInstanceEdges(t *testing.T) {
	cfg := &ConnectionConfig{Host: "localhost", Port: "3306", UserName: "user", Password: "pass", DbName: "db"}
	connector := new(MockedMySqlDbConnector)
	mockedDb := new(MockedMySqlDb)
	mockedDb.On("Ping").Return(nil)
	connector.On("Open", mock.Anything, mock.Anything).Return(mockedDb, nil)

	var nilCtx context.Context
	db, err := newInstance(nilCtx, connector, cfg)
	assert.NoError(t, err)
	assert.Equal(t, mockedDb, db)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	db, err = newInstance(cancelled, connector, cfg)
	assert.Error(t, err)
	assert.Nil(t, db)
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", "invalid:invalid@tcp(127.0.0.1:0)/invalid")
	require.NoError(t, err)
	return db
}

func TestMySqlDb_Methods(t *testing.T) {
	db := openTestDB(t)
	defer func() {
		require.NoError(t, db.Close())
	}()

	mdb := &MySqlDb{DB: db}

	assert.Error(t, mdb.Ping(context.Background()))

	_, err := mdb.Exec(context.Background(), "SELECT 1")
	assert.Error(t, err)

	_, err = mdb.Exec(context.Background(), "SELECT 1")
	assert.Error(t, err)

	_, err = mdb.Query(context.Background(), "SELECT 1")
	assert.Error(t, err)

	_, err = mdb.Query(context.Background(), "SELECT 1")
	assert.Error(t, err)

	row := mdb.QueryRow(context.Background(), "SELECT 1")
	assert.NotNil(t, row)

	mdb.SetConnMaxLifetime(time.Second)
	mdb.SetMaxIdleConns(1)
	mdb.SetMaxOpenConns(2)
	mdb.SetConnMaxIdleTime(time.Second)

	_ = mdb.Stats()

	_, err = mdb.Begin()
	assert.Error(t, err)

	_, err = mdb.BeginTx(context.Background(), nil)
	assert.Error(t, err)

	_, err = mdb.Prepare(context.Background(), "SELECT 1")
	assert.Error(t, err)

	_, err = mdb.Prepare(context.Background(), "SELECT 1")
	assert.Error(t, err)

	assert.NotNil(t, mdb.Driver())

	_, err = mdb.Conn(context.Background())
	assert.Error(t, err)

	assert.NoError(t, mdb.Close())
}
