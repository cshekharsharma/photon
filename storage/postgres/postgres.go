package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// DriverTypePostgres is the sql driver name registered by pgx stdlib.
const DriverTypePostgres = "pgx"

// PostgresDbConnectorInterface defines behavior for opening a connection.
type PostgresDbConnectorInterface interface {
	Open(driverName string, dataSourceName string) (PostgresDbInterface, error)
}

// PostgresDbConnector opens *sql.DB using sql.Open.
type PostgresDbConnector struct {
	DB *PostgresDb
}

func (pdbc *PostgresDbConnector) Open(driverName, dataSourceName string) (PostgresDbInterface, error) {
	db, err := sql.Open(driverName, dataSourceName)
	pdbc.DB = &PostgresDb{DB: db}
	return pdbc.DB, err
}

// PostgresDbInterface is intentionally broad enough to support DBContext without wrapper traps.
// PostgresDb embeds *sql.DB, so it satisfies this without re-implementing methods.
//
// Raw() exists as an escape hatch for tooling/migrations/sqlc/etc.
type PostgresDbInterface interface {
	Close() error
	Ping(ctx context.Context) error

	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)

	Query(ctx context.Context, query string, args ...any) (*sql.Rows, error)

	QueryRow(ctx context.Context, query string, args ...any) *sql.Row

	Prepare(ctx context.Context, query string) (*sql.Stmt, error)

	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)

	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	SetConnMaxIdleTime(d time.Duration)
	Stats() sql.DBStats

	Raw() *sql.DB
}

// PostgresDb wraps *sql.DB while keeping Photon's DB surface context-aware.
type PostgresDb struct {
	DB *sql.DB
}

func (pdb *PostgresDb) Close() error { return pdb.DB.Close() }
func (pdb *PostgresDb) Ping(ctx context.Context) error {
	return pdb.DB.PingContext(ctx)
}
func (pdb *PostgresDb) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return pdb.DB.ExecContext(ctx, query, args...)
}
func (pdb *PostgresDb) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return pdb.DB.QueryContext(ctx, query, args...)
}
func (pdb *PostgresDb) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return pdb.DB.QueryRowContext(ctx, query, args...)
}
func (pdb *PostgresDb) Prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	return pdb.DB.PrepareContext(ctx, query)
}
func (pdb *PostgresDb) Begin() (*sql.Tx, error) { return pdb.DB.Begin() }
func (pdb *PostgresDb) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return pdb.DB.BeginTx(ctx, opts)
}
func (pdb *PostgresDb) SetConnMaxLifetime(d time.Duration) { pdb.DB.SetConnMaxLifetime(d) }
func (pdb *PostgresDb) SetMaxIdleConns(n int)              { pdb.DB.SetMaxIdleConns(n) }
func (pdb *PostgresDb) SetMaxOpenConns(n int)              { pdb.DB.SetMaxOpenConns(n) }
func (pdb *PostgresDb) SetConnMaxIdleTime(d time.Duration) { pdb.DB.SetConnMaxIdleTime(d) }
func (pdb *PostgresDb) Stats() sql.DBStats                 { return pdb.DB.Stats() }
func (pdb *PostgresDb) Raw() *sql.DB                       { return pdb.DB }

var (
	mu sync.RWMutex

	instances                  map[string]PostgresDbInterface
	connectionConfigMap        map[string]*ConnectionConfig
	connectBeforeWriteLockHook = func(clusterName string) {}
	parsePGXConfigHook         = pgx.ParseConfig
	buildSafeDSNHook           = buildSafeDSN
)

// SetConnectionConfig registers config for a cluster.
func SetConnectionConfig(clusterName string, config *ConnectionConfig) {
	mu.Lock()
	defer mu.Unlock()

	if connectionConfigMap == nil {
		connectionConfigMap = make(map[string]*ConnectionConfig)
	}
	connectionConfigMap[clusterName] = clonePostgresConnectionConfig(config)
}

func SetConnectionConfigE(clusterName string, config *ConnectionConfig) error {
	if clusterName == "" {
		return fmt.Errorf("postgres: cluster name is required")
	}
	if err := config.Validate(); err != nil {
		return err
	}

	SetConnectionConfig(clusterName, config)
	return nil
}

// Connect returns a singleton connection per clusterName (per process).
func Connect(connector PostgresDbConnectorInterface, clusterName string) (PostgresDbInterface, error) {
	if connector == nil {
		return nil, fmt.Errorf("postgres: connector is required")
	}

	mu.RLock()
	if instances != nil {
		if db := instances[clusterName]; db != nil {
			mu.RUnlock()
			return db, nil
		}
	}
	mu.RUnlock()

	connectBeforeWriteLockHook(clusterName)
	mu.Lock()
	defer mu.Unlock()

	if instances == nil {
		instances = make(map[string]PostgresDbInterface)
	}
	if db := instances[clusterName]; db != nil {
		return db, nil
	}

	cfg, ok := connectionConfigMap[clusterName]
	if !ok || cfg == nil {
		return nil, fmt.Errorf("initialise db config using postgres.SetConnectionConfig() before use")
	}

	db, err := newInstance(connector, cfg)
	if err != nil {
		return nil, err
	}

	instances[clusterName] = db
	return db, nil
}

// CloseCluster closes a specific cluster connection (useful for graceful shutdown / tests).
func CloseCluster(clusterName string) error {
	mu.Lock()
	defer mu.Unlock()

	if instances == nil || instances[clusterName] == nil {
		return nil
	}
	err := instances[clusterName].Close()
	delete(instances, clusterName)
	return err
}

// CloseAll closes all connections (best effort).
func CloseAll() error {
	mu.Lock()
	defer mu.Unlock()

	var firstErr error
	for k, db := range instances {
		if db == nil {
			delete(instances, k)
			continue
		}
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(instances, k)
	}
	return firstErr
}

func newInstance(connector PostgresDbConnectorInterface, cfg *ConnectionConfig) (PostgresDbInterface, error) {
	if connector == nil {
		return nil, fmt.Errorf("postgres: connector is required")
	}

	normalized, err := cfg.normalized()
	if err != nil {
		return nil, err
	}
	cfg = normalized

	// TODO(otel): span "postgres.connect" with host/db, sslmode, pgbouncer mode, pool sizes etc (no password!)
	dsn, err := buildSafeDSNHook(cfg)
	if err != nil {
		return nil, err
	}

	pgxCfg, err := parsePGXConfigHook(dsn)
	if err != nil {
		return nil, err
	}

	pgxCfg.ConnectTimeout = cfg.ConnectTimeout

	// Runtime params
	if pgxCfg.RuntimeParams == nil {
		pgxCfg.RuntimeParams = make(map[string]string, 8)
	}
	applyRuntimeParamDefaults(pgxCfg.RuntimeParams)
	for k, v := range cfg.RuntimeParams {
		pgxCfg.RuntimeParams[k] = v
	}

	// PgBouncer safety: transaction/statement pooling breaks server-side prepares.
	if cfg.PgBouncer == PgBouncerTransaction || cfg.PgBouncer == PgBouncerStatement {
		pgxCfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}

	// Register config with stdlib and open via database/sql.
	connStr := stdlib.RegisterConnConfig(pgxCfg)

	dbi, err := connector.Open(DriverTypePostgres, connStr)
	if err != nil {
		return nil, err
	}

	// Pool tuning
	if cfg.MaxOpenConn > 0 {
		dbi.SetMaxOpenConns(cfg.MaxOpenConn)
	}
	if cfg.MaxIdleConn > 0 {
		dbi.SetMaxIdleConns(cfg.MaxIdleConn)
	}
	dbi.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	dbi.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	// Ping with timeout so startup cannot hang forever.
	pingCtx, cancel := context.WithTimeout(context.Background(), cfg.PingTimeout)
	defer cancel()

	// TODO(otel): record ping latency and result
	if err := dbi.Ping(pingCtx); err != nil {
		_ = dbi.Close()
		return nil, err
	}

	return dbi, nil
}

func applyDefaults(cfg *ConnectionConfig) {
	if cfg.SSLMode == "" {
		cfg.SSLMode = "disable"
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if cfg.PingTimeout <= 0 {
		cfg.PingTimeout = 3 * time.Second
	}
	if cfg.ConnMaxLifetime <= 0 {
		cfg.ConnMaxLifetime = 60 * time.Minute
	}
	if cfg.ConnMaxIdleTime <= 0 {
		cfg.ConnMaxIdleTime = 10 * time.Minute
	}
	if cfg.DefaultQueryTimeout <= 0 {
		cfg.DefaultQueryTimeout = 15 * time.Second
	}
}

func applyRuntimeParamDefaults(m map[string]string) {
	// Safe defaults (do not override caller-provided values).
	if _, ok := m["timezone"]; !ok {
		m["timezone"] = "UTC"
	}
	// statement_timeout is useful but can be contentious; keep it optional.
	// If you want it by default, uncomment below and adjust.
	// if _, ok := m["statement_timeout"]; !ok {
	// 	m["statement_timeout"] = "30s"
	// }
}

func buildSafeDSN(cfg *ConnectionConfig) (string, error) {
	if err := cfg.Validate(); err != nil {
		return "", err
	}

	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.UserName, cfg.Password),
		Host:   net.JoinHostPort(cfg.Host, cfg.Port),
		Path:   cfg.DbName,
	}

	q := url.Values{}
	q.Set("sslmode", cfg.SSLMode)
	u.RawQuery = q.Encode()

	return u.String(), nil
}

func clonePostgresConnectionConfig(config *ConnectionConfig) *ConnectionConfig {
	if config == nil {
		return nil
	}
	clone := *config
	if config.RuntimeParams != nil {
		clone.RuntimeParams = make(map[string]string, len(config.RuntimeParams))
		for k, v := range config.RuntimeParams {
			clone.RuntimeParams[k] = v
		}
	}
	return &clone
}
