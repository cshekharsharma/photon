package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const stubDriverName = "postgres_stub_driver"

var stubDriverRegistered atomic.Bool

func registerStubDriverOnce() {
	if stubDriverRegistered.CompareAndSwap(false, true) {
		sql.Register(stubDriverName, stubDriver{})
	}
}

type stubDriver struct{}

func (d stubDriver) Open(name string) (driver.Conn, error) {
	return &stubConn{}, nil
}

type stubConn struct{}

func (c *stubConn) Prepare(query string) (driver.Stmt, error) { return &stubStmt{}, nil }
func (c *stubConn) Close() error                              { return nil }
func (c *stubConn) Begin() (driver.Tx, error)                 { return &stubTx{}, nil }

// Optional interfaces used by database/sql when available
func (c *stubConn) Ping(ctx context.Context) error { return nil }

func (c *stubConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return &stubTx{}, nil
}

func (c *stubConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return stubResult{rows: 1, last: 7}, nil
}

func (c *stubConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return &stubRows{}, nil
}

type stubStmt struct{}

func (s *stubStmt) Close() error { return nil }
func (s *stubStmt) NumInput() int {
	return -1
}
func (s *stubStmt) Exec(args []driver.Value) (driver.Result, error) {
	return stubResult{rows: 2, last: 8}, nil
}
func (s *stubStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &stubRows{}, nil
}

type stubTx struct{}

func (t *stubTx) Commit() error   { return nil }
func (t *stubTx) Rollback() error { return nil }

type stubRows struct{}

func (r *stubRows) Columns() []string { return []string{"a"} }
func (r *stubRows) Close() error      { return nil }
func (r *stubRows) Next(dest []driver.Value) error {
	return io.EOF
}

type stubResult struct {
	rows int64
	last int64
}

func (r stubResult) LastInsertId() (int64, error) { return r.last, nil }
func (r stubResult) RowsAffected() (int64, error) { return r.rows, nil }

type mockConnector struct {
	openCalled int
	openErr    error
	dbToReturn PostgresDbInterface
}

func (m *mockConnector) Open(driverName string, dataSourceName string) (PostgresDbInterface, error) {
	m.openCalled++
	if m.openErr != nil {
		return nil, m.openErr
	}
	return m.dbToReturn, nil
}

type mockPgDB struct {
	closeCalled int
	closeErr    error

	pingCtxCalled int
	pingCtxErr    error

	maxOpenSet bool
	maxIdleSet bool
	lifeSet    bool
	idleSet    bool
}

func (m *mockPgDB) Close() error {
	m.closeCalled++
	return m.closeErr
}
func (m *mockPgDB) Ping(ctx context.Context) error {
	m.pingCtxCalled++
	return m.pingCtxErr
}

func (m *mockPgDB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return stubResult{rows: 1, last: 1}, nil
}
func (m *mockPgDB) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return nil, nil
}
func (m *mockPgDB) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return &sql.Row{}
}
func (m *mockPgDB) Prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	return nil, nil
}
func (m *mockPgDB) Begin() (*sql.Tx, error) { return nil, nil }
func (m *mockPgDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return nil, nil
}
func (m *mockPgDB) SetConnMaxLifetime(d time.Duration) { m.lifeSet = true }
func (m *mockPgDB) SetMaxIdleConns(n int)              { m.maxIdleSet = true }
func (m *mockPgDB) SetMaxOpenConns(n int)              { m.maxOpenSet = true }
func (m *mockPgDB) SetConnMaxIdleTime(d time.Duration) { m.idleSet = true }
func (m *mockPgDB) Stats() sql.DBStats                 { return sql.DBStats{} }
func (m *mockPgDB) Raw() *sql.DB                       { return nil }

func resetPostgresGlobals() {
	mu.Lock()
	defer mu.Unlock()
	instances = nil
	connectionConfigMap = nil
}

func TestPostgresDb_Forwards_All_Methods(t *testing.T) {
	registerStubDriverOnce()

	db, err := sql.Open(stubDriverName, "anything")
	if err != nil {
		t.Fatalf("sql.Open err: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("failed to close db: %v", err)
		}
	}()

	pdb := &PostgresDb{DB: db}

	// Close is tested via defer db.Close; still hit pdb.Close explicitly
	if err := pdb.Ping(context.Background()); err != nil {
		t.Fatalf("Ping err: %v", err)
	}

	res, err := pdb.Exec(context.Background(), "UPDATE t SET a=1")
	if err != nil {
		t.Fatalf("Exec err: %v", err)
	}
	_, _ = res.RowsAffected()
	_, _ = res.LastInsertId()

	res, err = pdb.Exec(context.Background(), "UPDATE t SET a=2")
	if err != nil {
		t.Fatalf("ExecContext err: %v", err)
	}
	_, _ = res.RowsAffected()

	rows, err := pdb.Query(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("Query err: %v", err)
	}
	if rows != nil {
		_ = rows.Close()
	}

	rows, err = pdb.Query(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("QueryContext err: %v", err)
	}
	if rows != nil {
		_ = rows.Close()
	}

	_ = pdb.QueryRow(context.Background(), "SELECT 1")

	st, err := pdb.Prepare(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("Prepare err: %v", err)
	}
	if st != nil {
		_ = st.Close()
	}

	st, err = pdb.Prepare(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("PrepareContext err: %v", err)
	}
	if st != nil {
		_ = st.Close()
	}

	tx, err := pdb.Begin()
	if err != nil {
		t.Fatalf("Begin err: %v", err)
	}
	if tx != nil {
		_ = tx.Rollback()
	}

	tx, err = pdb.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx err: %v", err)
	}
	if tx != nil {
		_ = tx.Rollback()
	}

	pdb.SetConnMaxLifetime(10 * time.Second)
	pdb.SetMaxIdleConns(2)
	pdb.SetMaxOpenConns(3)
	pdb.SetConnMaxIdleTime(5 * time.Second)
	_ = pdb.Stats()

	if pdb.Raw() != db {
		t.Fatalf("Raw() should return underlying *sql.DB")
	}

	if err := pdb.Close(); err != nil {
		t.Fatalf("Close err: %v", err)
	}
}

func TestSetConnectionConfig_And_Connect_Returns_Singleton(t *testing.T) {
	resetPostgresGlobals()

	cluster := "c1"
	cfg := &ConnectionConfig{
		Host: "localhost", Port: "5432", UserName: "u", Password: "p", DbName: "d",
		SSLMode: "disable",
		// Force some non-zero values to also exercise pool calls later in newInstance
		MaxOpenConn: 10, MaxIdleConn: 5,
		PingTimeout: 50 * time.Millisecond,
		RuntimeParams: map[string]string{
			"application_name": "test",
		},
	}

	SetConnectionConfig(cluster, cfg)

	db := &mockPgDB{}
	conn := &mockConnector{dbToReturn: db}

	// Insert instance directly first to cover RWMutex fast-path.
	mu.Lock()
	instances = map[string]PostgresDbInterface{cluster: db}
	mu.Unlock()

	got, err := Connect(conn, cluster)
	if err != nil {
		t.Fatalf("Connect err: %v", err)
	}
	if got != db {
		t.Fatalf("expected existing instance to be returned")
	}
	if conn.openCalled != 0 {
		t.Fatalf("expected connector.Open not called on fast-path")
	}

	// Reset instances so Connect goes through slow path and stores singleton
	mu.Lock()
	instances = nil
	mu.Unlock()

	got, err = Connect(conn, cluster)
	if err != nil {
		t.Fatalf("Connect err: %v", err)
	}
	if got == nil {
		t.Fatalf("expected db")
	}
	// Second connect should reuse stored instance (not call Open again)
	got2, err := Connect(conn, cluster)
	if err != nil {
		t.Fatalf("Connect err: %v", err)
	}
	if got2 != got {
		t.Fatalf("expected singleton instance")
	}
	if conn.openCalled != 1 {
		t.Fatalf("expected Open called exactly once, got %d", conn.openCalled)
	}
}

func TestConnect_DoubleCheckBranchAfterWriteLock(t *testing.T) {
	resetPostgresGlobals()

	cluster := "double-check-cluster"
	SetConnectionConfig(cluster, &ConnectionConfig{
		Host: "localhost", Port: "5432", UserName: "u", Password: "p", DbName: "d", SSLMode: "disable",
	})

	existing := &mockPgDB{}
	origHook := connectBeforeWriteLockHook
	defer func() { connectBeforeWriteLockHook = origHook }()
	connectBeforeWriteLockHook = func(clusterName string) {
		mu.Lock()
		if instances == nil {
			instances = make(map[string]PostgresDbInterface)
		}
		instances[clusterName] = existing
		mu.Unlock()
	}

	conn := &mockConnector{dbToReturn: &mockPgDB{}}
	got, err := Connect(conn, cluster)
	if err != nil {
		t.Fatalf("Connect err: %v", err)
	}
	if got != existing {
		t.Fatalf("expected existing instance from second check")
	}
	if conn.openCalled != 0 {
		t.Fatalf("expected Open not called, got %d", conn.openCalled)
	}
}

func TestConnect_NewInstanceError(t *testing.T) {
	resetPostgresGlobals()

	cluster := "connect-open-error"
	SetConnectionConfig(cluster, &ConnectionConfig{
		Host: "localhost", Port: "5432", UserName: "u", Password: "p", DbName: "d", SSLMode: "disable",
	})

	conn := &mockConnector{openErr: errors.New("open fail")}
	if _, err := Connect(conn, cluster); err == nil {
		t.Fatalf("expected open error")
	}
}

func TestConnect_Errors_When_Config_Missing(t *testing.T) {
	resetPostgresGlobals()

	conn := &mockConnector{dbToReturn: &mockPgDB{}}
	_, err := Connect(conn, "nope")
	if err == nil {
		t.Fatalf("expected error when config missing")
	}
}

func TestConnect_Errors_When_ConnectorNil(t *testing.T) {
	resetPostgresGlobals()

	SetConnectionConfig("c", &ConnectionConfig{Host: "localhost", Port: "5432", UserName: "u", DbName: "d"})
	if _, err := Connect(nil, "c"); err == nil {
		t.Fatal("expected nil connector error")
	}
}

func TestSetConnectionConfigEValidation(t *testing.T) {
	resetPostgresGlobals()

	valid := &ConnectionConfig{Host: "localhost", Port: "5432", UserName: "u", DbName: "d"}
	if err := SetConnectionConfigE("", valid); err == nil {
		t.Fatal("expected cluster error")
	}
	if err := SetConnectionConfigE("valid", valid); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	invalids := []*ConnectionConfig{
		nil,
		{Host: "localhost", Port: "bad", UserName: "u", DbName: "d"},
		{Host: "localhost", Port: "5432", UserName: "u", DbName: "d", SSLMode: "bad"},
		{Host: "localhost", Port: "5432", UserName: "u", DbName: "d", PgBouncer: PgBouncerMode("bad")},
		{Host: "localhost", Port: "5432", UserName: "u", DbName: "d", MaxOpenConn: -1},
		{Host: "localhost", Port: "5432", UserName: "u", DbName: "d", MaxIdleConn: -1},
		{Host: "localhost", Port: "5432", UserName: "u", DbName: "d", MaxOpenConn: 1, MaxIdleConn: 2},
		{Host: "localhost", Port: "5432", UserName: "u", DbName: "d", ConnMaxLifetime: -1},
		{Host: "localhost", Port: "5432", UserName: "u", DbName: "d", ConnMaxIdleTime: -1},
		{Host: "localhost", Port: "5432", UserName: "u", DbName: "d", ConnectTimeout: -1},
		{Host: "localhost", Port: "5432", UserName: "u", DbName: "d", PingTimeout: -1},
		{Host: "localhost", Port: "5432", UserName: "u", DbName: "d", DefaultQueryTimeout: -1},
	}
	for _, cfg := range invalids {
		if err := SetConnectionConfigE("bad", cfg); err == nil {
			t.Fatalf("expected validation error for %#v", cfg)
		}
	}
}

func TestClonePostgresConnectionConfigNil(t *testing.T) {
	if clone := clonePostgresConnectionConfig(nil); clone != nil {
		t.Fatalf("expected nil clone, got %#v", clone)
	}
}

func TestCloseCluster_Branches(t *testing.T) {
	resetPostgresGlobals()

	// instances nil => nil
	if err := CloseCluster("c"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// present but cluster missing => nil
	mu.Lock()
	instances = map[string]PostgresDbInterface{"x": &mockPgDB{}}
	mu.Unlock()

	if err := CloseCluster("c"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	db := &mockPgDB{}
	mu.Lock()
	instances = map[string]PostgresDbInterface{"c": db}
	mu.Unlock()

	if err := CloseCluster("c"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if db.closeCalled != 1 {
		t.Fatalf("expected Close called")
	}
	mu.RLock()
	_, ok := instances["c"]
	mu.RUnlock()
	if ok {
		t.Fatalf("expected instance deleted")
	}
}

func TestCloseAll_Branches(t *testing.T) {
	resetPostgresGlobals()

	dbErr := &mockPgDB{closeErr: errors.New("close fail")}
	dbOK := &mockPgDB{}
	mu.Lock()
	instances = map[string]PostgresDbInterface{
		"nil": nil,
		"e":   dbErr,
		"ok":  dbOK,
	}
	mu.Unlock()

	err := CloseAll()
	if err == nil {
		t.Fatalf("expected firstErr from close")
	}
	if dbErr.closeCalled != 1 || dbOK.closeCalled != 1 {
		t.Fatalf("expected both Close called once")
	}

	mu.RLock()
	defer mu.RUnlock()
	if len(instances) != 0 {
		t.Fatalf("expected instances cleared, got %d", len(instances))
	}
}

func TestBuildSafeDSN_Branches(t *testing.T) {
	_, err := buildSafeDSN(&ConnectionConfig{})
	if err == nil {
		t.Fatalf("expected error for missing required fields")
	}

	_, err = buildSafeDSN(&ConnectionConfig{
		Host: "h", Port: "bad", UserName: "u", DbName: "d",
	})
	if err == nil {
		t.Fatalf("expected error for invalid port")
	}

	dsn, err := buildSafeDSN(&ConnectionConfig{
		Host: "localhost", Port: "5432", UserName: "u", Password: "p", DbName: "db",
		SSLMode: "require",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if dsn == "" {
		t.Fatalf("expected non-empty DSN")
	}
}

func TestApplyDefaults_And_RuntimeParamDefaults(t *testing.T) {
	cfg := &ConnectionConfig{}
	applyDefaults(cfg)

	if cfg.SSLMode != "disable" {
		t.Fatalf("expected default SSLMode disable")
	}
	if cfg.ConnectTimeout <= 0 || cfg.PingTimeout <= 0 || cfg.ConnMaxLifetime <= 0 || cfg.ConnMaxIdleTime <= 0 || cfg.DefaultQueryTimeout <= 0 {
		t.Fatalf("expected timeout defaults")
	}

	// Ensure applyDefaults doesn't clobber pre-set values
	cfg2 := &ConnectionConfig{
		SSLMode:             "require",
		ConnectTimeout:      1 * time.Second,
		PingTimeout:         2 * time.Second,
		ConnMaxLifetime:     3 * time.Second,
		ConnMaxIdleTime:     4 * time.Second,
		DefaultQueryTimeout: 5 * time.Second,
	}
	applyDefaults(cfg2)
	if cfg2.SSLMode != "require" ||
		cfg2.ConnectTimeout != 1*time.Second ||
		cfg2.PingTimeout != 2*time.Second ||
		cfg2.ConnMaxLifetime != 3*time.Second ||
		cfg2.ConnMaxIdleTime != 4*time.Second ||
		cfg2.DefaultQueryTimeout != 5*time.Second {
		t.Fatalf("expected applyDefaults to keep explicit values")
	}

	// runtime defaults: timezone missing => set UTC
	m := map[string]string{}
	applyRuntimeParamDefaults(m)
	if m["timezone"] != "UTC" {
		t.Fatalf("expected timezone=UTC default")
	}

	// runtime defaults: timezone present => unchanged
	m2 := map[string]string{"timezone": "Asia/Kolkata"}
	applyRuntimeParamDefaults(m2)
	if m2["timezone"] != "Asia/Kolkata" {
		t.Fatalf("expected timezone preserved")
	}
}

func TestNewInstance_Returns_Error_When_BuildSafeDSN_Fails(t *testing.T) {
	resetPostgresGlobals()

	// missing required fields => buildSafeDSN fails
	conn := &mockConnector{dbToReturn: &mockPgDB{}}
	_, err := newInstance(conn, &ConnectionConfig{})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestNewInstance_Returns_Error_When_ConnectorNil(t *testing.T) {
	_, err := newInstance(nil, &ConnectionConfig{Host: "localhost", Port: "5432", UserName: "u", DbName: "d"})
	if err == nil {
		t.Fatal("expected nil connector error")
	}
}

func TestNewInstance_Returns_Error_When_BuildSafeDSNHookFails(t *testing.T) {
	orig := buildSafeDSNHook
	defer func() { buildSafeDSNHook = orig }()
	buildSafeDSNHook = func(*ConnectionConfig) (string, error) {
		return "", errors.New("dsn fail")
	}

	_, err := newInstance(&mockConnector{dbToReturn: &mockPgDB{}}, &ConnectionConfig{
		Host: "localhost", Port: "5432", UserName: "u", DbName: "d",
	})
	if err == nil {
		t.Fatal("expected dsn error")
	}
}

func TestNewInstance_DoesNotMutateCallerConfig(t *testing.T) {
	resetPostgresGlobals()

	cfg := &ConnectionConfig{
		Host:          "localhost",
		Port:          "5432",
		UserName:      "u",
		Password:      "p",
		DbName:        "d",
		RuntimeParams: map[string]string{"application_name": "test"},
	}
	db := &mockPgDB{}
	conn := &mockConnector{dbToReturn: db}

	if _, err := newInstance(conn, cfg); err != nil {
		t.Fatalf("newInstance err: %v", err)
	}
	if cfg.SSLMode != "" || cfg.ConnectTimeout != 0 || cfg.PingTimeout != 0 {
		t.Fatalf("expected caller config to remain unmodified: %#v", cfg)
	}
	if _, ok := cfg.RuntimeParams["timezone"]; ok {
		t.Fatal("expected caller runtime params to remain unmodified")
	}
}

func TestNewInstance_Returns_Error_When_PgxParseConfig_Fails(t *testing.T) {
	resetPostgresGlobals()

	orig := parsePGXConfigHook
	defer func() { parsePGXConfigHook = orig }()
	parsePGXConfigHook = func(string) (*pgx.ConnConfig, error) {
		return nil, errors.New("parse fail")
	}

	conn := &mockConnector{dbToReturn: &mockDB{}}
	cfg := &ConnectionConfig{
		Host:     "localhost",
		Port:     "5432",
		UserName: "u",
		Password: "p",
		DbName:   "d",
		SSLMode:  "disable",
	}
	_, err := newInstance(conn, cfg)
	if err == nil {
		t.Fatalf("expected pgx.ParseConfig error")
	}
}

func TestNewInstance_InitializesRuntimeParamsWhenNil(t *testing.T) {
	resetPostgresGlobals()

	conn := &mockConnector{dbToReturn: &mockPgDB{}}
	cfg := &ConnectionConfig{
		Host: "localhost", Port: "5432", UserName: "u", Password: "p", DbName: "d", SSLMode: "disable",
	}

	orig := parsePGXConfigHook
	defer func() { parsePGXConfigHook = orig }()
	parsePGXConfigHook = func(connString string) (*pgx.ConnConfig, error) {
		c, err := pgx.ParseConfig(connString)
		if err != nil {
			return nil, err
		}
		c.RuntimeParams = nil
		return c, nil
	}

	db, err := newInstance(conn, cfg)
	if err != nil {
		t.Fatalf("newInstance err: %v", err)
	}
	if db == nil {
		t.Fatalf("expected non-nil db")
	}
}

func TestNewInstance_OpenError_And_PingError_Branches(t *testing.T) {
	resetPostgresGlobals()

	// 1) connector.Open error
	openFail := &mockConnector{openErr: errors.New("open failed")}
	cfg := &ConnectionConfig{
		Host: "localhost", Port: "5432", UserName: "u", Password: "p", DbName: "d",
		SSLMode:       "disable",
		PingTimeout:   20 * time.Millisecond,
		RuntimeParams: map[string]string{},
	}
	_, err := newInstance(openFail, cfg)
	if err == nil {
		t.Fatalf("expected open error")
	}

	// 2) PingContext error => db.Close called and error returned
	db := &mockPgDB{pingCtxErr: errors.New("ping failed")}
	conn := &mockConnector{dbToReturn: db}
	cfg2 := &ConnectionConfig{
		Host: "localhost", Port: "5432", UserName: "u", Password: "p", DbName: "d",
		SSLMode:       "disable",
		PingTimeout:   20 * time.Millisecond,
		MaxOpenConn:   1,
		MaxIdleConn:   1,
		RuntimeParams: map[string]string{
			// leave timezone missing so applyRuntimeParamDefaults sets it
		},
	}
	_, err = newInstance(conn, cfg2)
	if err == nil {
		t.Fatalf("expected ping error")
	}
	if db.closeCalled != 1 {
		t.Fatalf("expected Close called on ping failure")
	}
	// pool tuning branches executed
	if !db.maxOpenSet || !db.maxIdleSet || !db.lifeSet || !db.idleSet {
		t.Fatalf("expected pool setters called")
	}
}

func TestNewInstance_PgBouncer_Mode_Sets_SimpleProtocol(t *testing.T) {
	resetPostgresGlobals()

	// We can't easily introspect pgxCfg.DefaultQueryExecMode after RegisterConnConfig,
	// but we CAN ensure we pass through the branch by setting PgBouncer mode.
	db := &mockPgDB{} // ping ok
	conn := &mockConnector{dbToReturn: db}

	cfg := &ConnectionConfig{
		Host: "localhost", Port: "5432", UserName: "u", Password: "p", DbName: "d",
		SSLMode:     "disable",
		PingTimeout: 20 * time.Millisecond,
		RuntimeParams: map[string]string{
			"application_name": "x",
		},
		// force branch
		PgBouncer: PgBouncerTransaction,
	}

	_, err := newInstance(conn, cfg)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if db.pingCtxCalled != 1 {
		t.Fatalf("expected PingContext called once")
	}
}

func TestPostgresDbConnector_Open(t *testing.T) {
	registerStubDriverOnce()

	pdbc := &PostgresDbConnector{}
	dbi, err := pdbc.Open(stubDriverName, "anything")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if dbi == nil || pdbc.DB == nil {
		t.Fatalf("expected db instance")
	}
	_ = dbi.Close()
}
