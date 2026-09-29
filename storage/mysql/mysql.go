// Package mysql provides utilities for establishing and managing connections
// to MySQL databases. It abstracts common database operations, allowing for
// consistent interactions across different parts of an application.
//
// The package defines a Database interface that can be used for various
// database operations like querying, executing commands, and managing connections.
// This makes it easier to swap out actual database implementations or to mock
// the database during testing.
//
// Usage:
//
// 1. Initialize the MySqlDbConnector and call the Open method to establish a connection.
// 2. Perform database operations using the MySqlDb methods that wrap standard sql.DB functions.
// 3. Use the Database interface for more abstract interactions or when mocking for tests.
//
// It's essential to manage database connections carefully, ensuring they are closed
// when no longer needed, and to be aware of the configuration concerning
// connection lifetimes and pool sizes.
package mysql

import (
	"context"
	"fmt"
	"sync"
	"time"

	"database/sql"
	"database/sql/driver"

	_ "github.com/go-sql-driver/mysql"
)

// *sql.DB Driver type for MYSQL
const DriverTypeMySQL = "mysql"

// MySqlDbConnectorInterface defines the behavior required for establishing connections
// with a MySQL database. Implementers of this interface are expected to provide
// the specific logic to open a connection based on the provided driver and data source name.
type MySqlDbConnectorInterface interface {
	Open(driverName string, dataSourceName string) (MySqlDbInterface, error)
}

// MySqlDbConnector serves as a wrapper around the MySqlDb type, providing an interface
// for establishing a connection to a MySQL database.
type MySqlDbConnector struct {

	// DB represents a wrapped instance of sql.DB for MySQL database operations.
	DB *MySqlDb
}

// Open establishes a new connection to a MySQL database using the specified driver and
// data source name. The function will create an instance of MySqlDb and assigns it to the
// DB field of the MySqlDbConnector.
//
// Parameters:
//   - driverName: Name of the driver used for the database connection.
//   - dataSourceName: Connection string that contains information about the database.
//
// Returns:
//   - A pointer to the MySqlDbInterface instance which wraps the sql.DB.
//   - An error if any issues were encountered while trying to establish the connection.
func (mdbc *MySqlDbConnector) Open(driverName string, dataSourceName string) (MySqlDbInterface, error) {
	connection, err := sql.Open(driverName, dataSourceName)
	mdbc.DB = &MySqlDb{DB: connection}

	return mdbc.DB, err
}

// MySqlDbInterface represents a generalized interface for interacting with a database.
// It abstracts common database operations, making it easier to work with
// different database systems and allowing for easier mocking in tests.
type MySqlDbInterface interface {
	Ping(ctx context.Context) error
	Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	SetConnMaxIdleTime(d time.Duration)
	Stats() sql.DBStats
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Prepare(ctx context.Context, query string) (*sql.Stmt, error)
	Driver() driver.Driver
	Conn(ctx context.Context) (*sql.Conn, error)
	Close() error
}

// MySqlDb serves as a wrapper around sql.DB, exposing a set of methods for MySQL
// database operations.
type MySqlDb struct {

	// DB is the underlying instance of sql.DB to which MySqlDb delegates its calls.
	DB *sql.DB
}

// Close terminates the database connection. It delegates the call to the underlying
// sql.DB's Close method.
//
// Returns:
// - An error if any issues were encountered while trying to close the connection.
func (mdb *MySqlDb) Close() error {
	return mdb.DB.Close()
}

// Ping checks the database connection for liveness. This is useful for ensuring that
// the connection to the database is still active and operational.
//
// Returns:
// - An error if the connection isn't alive or any other issues are encountered.
// Ping checks the database connection for liveness, with the given context.
// Useful when you want to provide a timeout or cancel the operation.
//
// Parameters:
//   - ctx: Context to use for the Ping operation.
//
// Returns:
//   - An error if the connection isn't alive, context times out, or any other issues arise.
func (mdb *MySqlDb) Ping(ctx context.Context) error {
	return mdb.DB.PingContext(ctx)
}

// Exec runs an SQL statement which doesn't return rows, like an INSERT, DELETE, or UPDATE.
//
// Parameters:
//   - query: The SQL query string to execute.
//   - args: Parameters for the SQL query.
//
// Returns:
//   - Result of the executed query.
//   - Any error encountered during execution.
func (mdb *MySqlDb) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return mdb.DB.ExecContext(ctx, query, args...)
}

// Query executes a provided SQL query with the given arguments and returns
// a set of rows from the database. The rows should be closed after usage.
// If no rows are found, it doesn't return an error but `rows.Next` will return `false`.
//
// Parameters:
//   - query: The SQL query string to execute.
//   - args: Parameters for the SQL query.
//
// Returns:
//   - A pointer to the retrieved rows.
//   - Any error encountered during execution.
func (mdb *MySqlDb) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return mdb.DB.QueryContext(ctx, query, args...)
}

// QueryRow executes a provided SQL query with the given arguments and returns
// a single row from the database. If no rows are found, it doesn't return an error.
// If the executed query returns more than one row, only the first row is returned.
//
// Parameters:
//   - query: The SQL query string to execute.
//   - args: Parameters for the SQL query.
//
// Returns:
//   - A pointer to the retrieved single row.
func (mdb *MySqlDb) QueryRow(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return mdb.DB.QueryRowContext(ctx, query, args...)
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
// If a connection is older than this duration, it'll be closed when it's returned to the pool.
// Expired connections may be closed lazily before reuse.
//
// Parameters:
//   - d: Duration value representing the maximum lifetime of a connection.
func (mdb *MySqlDb) SetConnMaxLifetime(d time.Duration) {
	mdb.DB.SetConnMaxLifetime(d)
}

// SetMaxIdleConns sets the maximum number of connections in the idle connection pool.
// If this value is exceeded at any point, the excess connections will be closed.
// If negative, there's no limit on the number of idle connections.
//
// Parameters:
//   - n: Maximum number of idle connections to be retained.

func (mdb *MySqlDb) SetMaxIdleConns(n int) {
	mdb.DB.SetMaxIdleConns(n)
}

// SetMaxOpenConns sets the maximum number of open connections to the database.
// If this limit is exceeded, new connections will be blocked until one of the
// existing connections is returned to the pool.
// If non-positive, there's no limit on the number of open connections.
//
// Parameters:
//   - n: Maximum number of open connections to the database.
func (mdb *MySqlDb) SetMaxOpenConns(n int) {
	mdb.DB.SetMaxOpenConns(n)
}

// SetConnMaxIdleTime sets the maximum amount of time a connection may be idle before being closed.
// Idle connections which exceed this duration will be closed lazily on their next use.
// If the value is zero, then idle connections are not closed based on the idle time.
//
// Parameters:
//   - d: Duration value representing the maximum idle time of a connection.
func (mdb *MySqlDb) SetConnMaxIdleTime(d time.Duration) {
	mdb.DB.SetConnMaxIdleTime(d)
}

// Stats retrieves and returns database statistics, such as the number of
// open connections and whether the database is idle.
//
// Returns:
//   - sql.DBStats: A structure containing the database's operational statistics.
func (mdb *MySqlDb) Stats() sql.DBStats {
	return mdb.DB.Stats()
}

// Begin starts and returns a new transaction. If an error occurs
// while initializing the transaction, it will be returned.
//
// Returns:
//   - *sql.Tx: A pointer to the new transaction.
//   - error: An error object detailing any issues starting the transaction.
func (mdb *MySqlDb) Begin() (*sql.Tx, error) {
	return mdb.DB.Begin()
}

// BeginTx starts a new transaction with the provided context and options.
// It allows for custom transaction isolation levels and readonly settings.
//
// Parameters:
//   - ctx: Context for the transaction, which can be used to cancel it.
//   - opts: Transaction options, such as isolation level.
//
// Returns:
//   - *sql.Tx: A pointer to the new transaction.
//   - error: An error object detailing any issues starting the transaction.
func (mdb *MySqlDb) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return mdb.DB.BeginTx(ctx, opts)
}

// Prepare creates a prepared statement for later queries or executions.
// The provided query may contain placeholders for binding parameters.
//
// Parameters:
//   - query: SQL query string possibly containing placeholders.
//
// Returns:
//   - *sql.Stmt: A statement object which can be executed with different parameters.
//   - error: An error object detailing any issues preparing the statement.
func (mdb *MySqlDb) Prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	return mdb.DB.PrepareContext(ctx, query)
}

// Driver returns the database's underlying driver.
//
// Returns:
//   - driver.Driver: The database's underlying driver.
func (mdb *MySqlDb) Driver() driver.Driver {
	return mdb.DB.Driver()
}

// Conn returns a single connection from the connection pool.
// The provided context can be used to cancel or time out the connection request.
//
// Parameters:
//   - ctx: Context for obtaining the connection.
//
// Returns:
//   - *sql.Conn: A single database connection.
//   - error: An error object detailing any issues obtaining the connection.
func (mdb *MySqlDb) Conn(ctx context.Context) (*sql.Conn, error) {
	return mdb.DB.Conn(ctx)
}

// TxF defines the interface for executing SQL statements within a transaction context.
// It mirrors the Exec method from *sql.Tx to allow mocking in unit tests.
type Tx interface {
	Exec(query string, args ...any) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	Query(query string, args ...any) (*sql.Rows, error)
}

// TxContext is an optional transaction extension for context-aware SQL operations.
// *sql.Tx implements these methods.
type TxContext interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

var (

	// mutex is used to ensure thread-safety when accessing the `instances` map.
	// It prevents concurrent modifications which can lead to race conditions.
	mutex sync.RWMutex

	// instances is a map that stores instances of MySqlDb.
	// The key is typically a unique identifier (e.g., connection string or name)
	// and the value is the corresponding MySqlDb instance.
	// This can be used for scenarios like connection pooling or managing multiple
	// MySqlDb instances.
	instances map[string]MySqlDbInterface
)

var connectionConfigMap map[string]*ConnectionConfig

func SetConnectionConfig(clusterName string, config *ConnectionConfig) {
	mutex.Lock()
	defer mutex.Unlock()

	if connectionConfigMap == nil {
		connectionConfigMap = make(map[string]*ConnectionConfig)
	}

	connectionConfigMap[clusterName] = cloneConnectionConfig(config)
}

// Connect establishes a connection to a database cluster specified by the
// given clusterName. If a connection to the cluster already exists, the
// existing connection is reused. This method uses a singleton pattern to
// ensure only one connection instance per clusterName.
//
// Parameters:
//   - clusterName: The name of the database cluster to connect to.
//
// Returns:
//   - *MySqlDb: A pointer to the established database connection. If a connection
//     to the given clusterName already exists, the existing connection
//     will be returned.
//   - error: An error object that describes the reason for any connection failures.
//     It returns nil if the connection was successful.
//
// Note: This method is thread-safe and uses mutexes to handle concurrent access.
// Connect establishes or reuses a MySQL connection for clusterName.
// The supplied context is honored while opening and pinging a new connection.
func Connect(ctx context.Context, connector MySqlDbConnectorInterface, clusterName string) (MySqlDbInterface, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var err error
	var newConn MySqlDbInterface

	mutex.Lock()
	defer mutex.Unlock()

	if instances == nil {
		instances = make(map[string]MySqlDbInterface)
	}
	if instances[clusterName] != nil {
		return instances[clusterName], nil
	}

	if connectionConfigMap == nil {
		return nil, fmt.Errorf("initialise db config using mysql.SetConnectionConfig() before use")
	}

	dbconfig, ok := connectionConfigMap[clusterName]
	if !ok {
		return nil, fmt.Errorf("initialise db config using mysql.SetConnectionConfig() before use")
	}

	newConn, err = newInstance(ctx, connector, dbconfig)
	if err != nil || newConn == nil {
		return nil, err
	}

	instances[clusterName] = newConn

	return newConn, nil
}

func newInstance(ctx context.Context, connector MySqlDbConnectorInterface, dbconfig *ConnectionConfig) (MySqlDbInterface, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	connString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
		dbconfig.UserName, dbconfig.Password, dbconfig.Host, dbconfig.Port, dbconfig.DbName)

	db, err := connector.Open(DriverTypeMySQL, connString)
	if err != nil {
		return nil, err
	}

	// Setting database connection settings
	db.SetMaxOpenConns(int(dbconfig.MaxOpenConn))
	db.SetMaxIdleConns(int(dbconfig.MaxIdleConn))

	db.SetConnMaxLifetime(time.Duration(int(dbconfig.MaxConnLifetime)) * time.Second)
	db.SetConnMaxIdleTime(time.Duration(int(dbconfig.ConnMaxIdleTime)) * time.Second)

	err = db.Ping(ctx)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func GetConnectionConfig(clusterName string) *ConnectionConfig {
	mutex.RLock()
	defer mutex.RUnlock()

	if connectionConfigMap == nil {
		return nil
	}
	return cloneConnectionConfig(connectionConfigMap[clusterName])
}

func cloneConnectionConfig(config *ConnectionConfig) *ConnectionConfig {
	if config == nil {
		return nil
	}
	copied := *config
	return &copied
}
