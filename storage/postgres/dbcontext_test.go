package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

type fakeResult struct {
	lastID int64
	rows   int64
}

func (r fakeResult) LastInsertId() (int64, error) {
	return r.lastID, nil
}

func (r fakeResult) RowsAffected() (int64, error) {
	return r.rows, nil
}

func resetGlobals() {
	mu.Lock()
	defer mu.Unlock()
	instances = nil
	connectionConfigMap = nil
}

type mockTxNoCtx struct {
	execCalled    int
	prepareCalled int
	queryCalled   int

	lastExecQuery string
	lastExecArgs  []any

	lastPrepareQuery string

	lastQueryQuery string
	lastQueryArgs  []any

	execRes sql.Result
	execErr error

	stmt *sql.Stmt
	perr error

	rows *sql.Rows
	qerr error
}

func (m *mockTxNoCtx) Exec(query string, args ...any) (sql.Result, error) {
	m.execCalled++
	m.lastExecQuery = query
	m.lastExecArgs = args
	return m.execRes, m.execErr
}

func (m *mockTxNoCtx) Prepare(query string) (*sql.Stmt, error) {
	m.prepareCalled++
	m.lastPrepareQuery = query
	return m.stmt, m.perr
}

func (m *mockTxNoCtx) Query(query string, args ...any) (*sql.Rows, error) {
	m.queryCalled++
	m.lastQueryQuery = query
	m.lastQueryArgs = args
	return m.rows, m.qerr
}

type mockTxWithCtx struct {
	mockTxNoCtx

	execCtxCalled    int
	prepareCtxCalled int
	queryCtxCalled   int

	lastExecCtx context.Context
	lastQryCtx  context.Context
	lastPrepCtx context.Context
}

func (m *mockTxWithCtx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	m.execCtxCalled++
	m.lastExecCtx = ctx
	m.lastExecQuery = query
	m.lastExecArgs = args
	return m.execRes, m.execErr
}

func (m *mockTxWithCtx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	m.prepareCtxCalled++
	m.lastPrepCtx = ctx
	m.lastPrepareQuery = query
	return m.stmt, m.perr
}

func (m *mockTxWithCtx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	m.queryCtxCalled++
	m.lastQryCtx = ctx
	m.lastQueryQuery = query
	m.lastQueryArgs = args
	return m.rows, m.qerr
}

type mockDB struct {
	execCtxCalled    int
	prepareCtxCalled int
	queryCtxCalled   int

	lastExecCtx context.Context
	lastQryCtx  context.Context
	lastPrepCtx context.Context

	lastExecQuery  string
	lastExecArgs   []any
	lastQueryQuery string
	lastQueryArgs  []any
	lastPrepQuery  string

	execRes sql.Result
	execErr error

	stmt *sql.Stmt
	perr error

	rows *sql.Rows
	qerr error
}

func (m *mockDB) Close() error {
	return nil
}
func (m *mockDB) Ping(ctx context.Context) error {
	return nil
}

func (m *mockDB) SetConnMaxLifetime(d time.Duration) {}
func (m *mockDB) SetMaxIdleConns(n int)              {}
func (m *mockDB) SetMaxOpenConns(n int)              {}
func (m *mockDB) SetConnMaxIdleTime(d time.Duration) {}

func (m *mockDB) Stats() sql.DBStats {
	return sql.DBStats{}
}

func (m *mockDB) Raw() *sql.DB {
	return nil
}

func (m *mockDB) Begin() (*sql.Tx, error) {
	return nil, nil
}

func (m *mockDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return nil, nil
}

func (m *mockDB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	m.execCtxCalled++
	m.lastExecCtx = ctx
	m.lastExecQuery = query
	m.lastExecArgs = args
	return m.execRes, m.execErr
}

func (m *mockDB) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	m.queryCtxCalled++
	m.lastQryCtx = ctx
	m.lastQueryQuery = query
	m.lastQueryArgs = args
	return m.rows, m.qerr
}

func (m *mockDB) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return &sql.Row{}
}

func (m *mockDB) Prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	m.prepareCtxCalled++
	m.lastPrepCtx = ctx
	m.lastPrepQuery = query
	return m.stmt, m.perr
}

func TestDBContext_Exec(t *testing.T) {
	resetGlobals()

	var gotCtx context.Context
	ctx := &DBContext{
		DefaultTimeout: 25 * time.Millisecond,
		ExecFn: func(c context.Context, q string, args ...any) (sql.Result, error) {
			gotCtx = c
			return fakeResult{rows: 9}, nil
		},
	}

	res, err := ctx.Exec(context.Background(), "UPDATE x SET y=1", 1, 2)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	ra, _ := res.RowsAffected()
	if ra != 9 {
		t.Fatalf("expected rows=9, got %d", ra)
	}

	// Exec uses context.Background() + DefaultTimeout => must have deadline
	if _, ok := gotCtx.Deadline(); !ok {
		t.Fatalf("expected deadline to be applied by DefaultTimeout")
	}
}

func TestDBContext_Query(t *testing.T) {
	resetGlobals()

	var gotCtx context.Context
	ctx := &DBContext{
		DefaultTimeout: 25 * time.Millisecond,
		QueryFn: func(c context.Context, q string, args ...any) (*sql.Rows, error) {
			gotCtx = c
			return nil, nil
		},
	}
	_, err := ctx.Query(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if _, ok := gotCtx.Deadline(); !ok {
		t.Fatalf("expected deadline to be applied by DefaultTimeout")
	}
}

func TestDBContext_Prepare(t *testing.T) {
	resetGlobals()

	var gotCtx context.Context
	ctx := &DBContext{
		DefaultTimeout: 25 * time.Millisecond,
		PrepareFn: func(c context.Context, q string) (*sql.Stmt, error) {
			gotCtx = c
			return nil, nil
		},
	}

	_, err := ctx.Prepare(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if _, ok := gotCtx.Deadline(); !ok {
		t.Fatalf("expected deadline to be applied by DefaultTimeout")
	}
}

func TestDBContext_ExecContext(t *testing.T) {
	resetGlobals()

	// Cover ExecFn path + cancel != nil defer path
	var sawDeadline bool
	ctx := &DBContext{
		DefaultTimeout: 25 * time.Millisecond,
		ExecFn: func(c context.Context, q string, args ...any) (sql.Result, error) {
			_, sawDeadline = c.Deadline()
			return fakeResult{rows: 1}, nil
		},
	}

	_, err := ctx.Exec(context.Background(), "DELETE FROM t WHERE id=$1", 7)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if !sawDeadline {
		t.Fatalf("expected default timeout deadline")
	}

	// Cover path where context already has deadline => cancel == nil
	dctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ctx = &DBContext{
		DefaultTimeout: 25 * time.Millisecond,
		ExecFn: func(c context.Context, q string, args ...any) (sql.Result, error) {
			// should keep existing deadline
			_, ok := c.Deadline()
			if !ok {
				t.Fatalf("expected existing deadline to remain")
			}
			return fakeResult{rows: 2}, nil
		},
	}
	_, err = ctx.Exec(dctx, "UPDATE t SET a=1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestDBContext_PrepareContext(t *testing.T) {
	resetGlobals()

	ctx := &DBContext{
		DefaultTimeout: 25 * time.Millisecond,
		PrepareFn: func(c context.Context, q string) (*sql.Stmt, error) {
			return nil, nil
		},
	}
	_, err := ctx.Prepare(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestDBContext_PrepareContext_FallbackWithNilContext(t *testing.T) {
	resetGlobals()

	db := &mockDB{}
	ctx := &DBContext{
		Conn:           db,
		DefaultTimeout: 0,
	}

	var nilCtx context.Context = nil
	_, err := ctx.Prepare(nilCtx, "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if db.prepareCtxCalled != 1 {
		t.Fatalf("expected conn.PrepareContext called once")
	}
	if db.lastPrepCtx == nil {
		t.Fatalf("expected non-nil context fallback from nil input")
	}
}

func TestDBContext_QueryContext(t *testing.T) {
	resetGlobals()

	ctx := &DBContext{
		DefaultTimeout: 25 * time.Millisecond,
		QueryFn: func(c context.Context, q string, args ...any) (*sql.Rows, error) {
			return nil, nil
		},
	}
	_, err := ctx.Query(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestDBContext_withDefaultTimeout(t *testing.T) {
	resetGlobals()

	d := &DBContext{DefaultTimeout: 0}
	c, cancel := d.withDefaultTimeout(context.TODO())

	if c == nil {
		t.Fatal("expected non-nil context")
	}
	if cancel != nil {
		t.Fatal("expected nil cancel when DefaultTimeout <= 0")
	}

	d = &DBContext{DefaultTimeout: 10 * time.Millisecond}
	c, cancel = d.withDefaultTimeout(context.Background())
	if cancel == nil {
		t.Fatal("expected cancel when timeout is applied")
	}

	cancel()

	if _, ok := c.Deadline(); !ok {
		t.Fatal("expected deadline")
	}

	dctx, dcancel := context.WithTimeout(context.Background(), time.Second)
	defer dcancel()

	_, cancel = d.withDefaultTimeout(dctx)
	if cancel != nil {
		t.Fatal("expected nil cancel when ctx already has deadline")
	}
}

func TestDBContext_validateResolution(t *testing.T) {
	resetGlobals()

	ctx := &DBContext{}
	if err := ctx.validateResolution(); err == nil {
		t.Fatal("expected error when Tx/Conn/Cluster all missing")
	}

	ctx = &DBContext{Cluster: "c1"}
	if err := ctx.validateResolution(); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestDBContext_resolveConn(t *testing.T) {
	resetGlobals()

	ctx := &DBContext{}
	_, err := ctx.resolveConn()
	if err == nil {
		t.Fatal("expected error")
	}

	ctx = &DBContext{Tx: &mockTxNoCtx{}}
	_, err = ctx.resolveConn()
	if err == nil {
		t.Fatal("expected error when Tx present")
	}

	ctx = &DBContext{Conn: &mockDB{}}
	_, err = ctx.resolveConn()
	if err == nil {
		t.Fatal("expected error when Conn present")
	}

	ctx = &DBContext{Cluster: "missing"}
	_, err = ctx.resolveConn()
	if err == nil {
		t.Fatal("expected Connect error for missing cluster config")
	}
}

func TestDBContext_exec(t *testing.T) {
	resetGlobals()

	ctx := &DBContext{}
	_, err := ctx.exec(context.Background(), "x")
	if err == nil {
		t.Fatal("expected error")
	}

	// Tx without TxContext => uses Exec()
	tx := &mockTxNoCtx{execRes: fakeResult{rows: 3}}
	ctx = &DBContext{Tx: tx}
	res, err := ctx.exec(context.Background(), "UPDATE t SET a=1", 1)

	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if tx.execCalled != 1 {
		t.Fatalf("expected tx.Exec to be called")
	}

	ra, _ := res.RowsAffected()
	if ra != 3 {
		t.Fatalf("expected rows=3")
	}

	tx2 := &mockTxWithCtx{mockTxNoCtx: mockTxNoCtx{execRes: fakeResult{rows: 4}}}
	ctx = &DBContext{Tx: tx2}

	_, err = ctx.exec(context.Background(), "DELETE FROM t", 2)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if tx2.execCtxCalled != 1 {
		t.Fatalf("expected tx.ExecContext to be called")
	}

	db := &mockDB{execRes: fakeResult{rows: 5}}
	ctx = &DBContext{Conn: db}
	_, err = ctx.exec(context.Background(), "INSERT INTO t(a) VALUES($1)", 1)

	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if db.execCtxCalled != 1 {
		t.Fatalf("expected conn.ExecContext to be called")
	}

	cluster := "c1"
	mu.Lock()
	instances = map[string]PostgresDbInterface{
		cluster: &mockDB{execRes: fakeResult{rows: 6}},
	}

	mu.Unlock()

	ctx = &DBContext{Cluster: cluster}
	res, err = ctx.exec(context.Background(), "UPDATE t SET b=2")

	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	ra, _ = res.RowsAffected()
	if ra != 6 {
		t.Fatalf("expected rows=6")
	}

	resetGlobals()
	ctx = &DBContext{Cluster: "missing"}
	_, err = ctx.exec(context.Background(), "x")

	if err == nil {
		t.Fatal("expected error from Connect")
	}
}

func TestDBContext_prepare(t *testing.T) {
	resetGlobals()

	ctx := &DBContext{}
	_, err := ctx.prepare(context.Background(), "x")

	if err == nil {
		t.Fatal("expected error")
	}

	tx := &mockTxNoCtx{}
	ctx = &DBContext{Tx: tx}
	_, err = ctx.prepare(context.Background(), "SELECT 1")

	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if tx.prepareCalled != 1 {
		t.Fatalf("expected tx.Prepare(context.Background(), ) called")
	}

	tx2 := &mockTxWithCtx{}
	ctx = &DBContext{Tx: tx2}
	_, err = ctx.prepare(context.Background(), "SELECT 2")

	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if tx2.prepareCtxCalled != 1 {
		t.Fatalf("expected tx.Prepare(context.Background(), ) called")
	}

	db := &mockDB{}
	ctx = &DBContext{Conn: db}
	_, err = ctx.prepare(context.Background(), "SELECT 3")

	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if db.prepareCtxCalled != 1 {
		t.Fatalf("expected conn.PrepareContext called")
	}

	cluster := "c2"
	mu.Lock()

	instances = map[string]PostgresDbInterface{
		cluster: &mockDB{},
	}
	mu.Unlock()

	ctx = &DBContext{Cluster: cluster}
	_, err = ctx.prepare(context.Background(), "SELECT 4")

	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	resetGlobals()
	ctx = &DBContext{Cluster: "missing"}
	_, err = ctx.prepare(context.Background(), "x")

	if err == nil {
		t.Fatal("expected error from Connect")
	}
}

func TestDBContext_query(t *testing.T) {
	resetGlobals()

	ctx := &DBContext{}
	_, err := ctx.query(context.Background(), "x")
	if err == nil {
		t.Fatal("expected error")
	}

	tx := &mockTxNoCtx{}
	ctx = &DBContext{Tx: tx}
	_, err = ctx.query(context.Background(), "SELECT 1", 1)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if tx.queryCalled != 1 {
		t.Fatalf("expected tx.Query(context.Background(), ) called")
	}

	tx2 := &mockTxWithCtx{}
	ctx = &DBContext{Tx: tx2}
	_, err = ctx.query(context.Background(), "SELECT 2", 2)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if tx2.queryCtxCalled != 1 {
		t.Fatalf("expected tx.Query(context.Background(), ) called")
	}

	db := &mockDB{}
	ctx = &DBContext{Conn: db}
	_, err = ctx.query(context.Background(), "SELECT 3")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if db.queryCtxCalled != 1 {
		t.Fatalf("expected conn.QueryContext called")
	}

	cluster := "c3"
	mu.Lock()
	instances = map[string]PostgresDbInterface{
		cluster: &mockDB{},
	}
	mu.Unlock()

	ctx = &DBContext{Cluster: cluster}
	_, err = ctx.query(context.Background(), "SELECT 4")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	resetGlobals()
	ctx = &DBContext{Cluster: "missing"}
	_, err = ctx.query(context.Background(), "x")
	if err == nil {
		t.Fatal("expected error from Connect")
	}
}

func TestDBContext_Errors_areMeaningful(t *testing.T) {
	resetGlobals()
	ctx := &DBContext{}
	_, err := ctx.Exec(context.Background(), "x")
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() == "" {
		t.Fatal("expected non-empty error")
	}
	if !errors.Is(err, err) {
		t.Fatal("unexpected")
	}
}
