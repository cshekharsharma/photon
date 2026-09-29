package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

type postgresErrCloser struct{}

func (postgresErrCloser) Close() error {
	return errors.New("close failed")
}

func TestCloseSQLCloser_IgnoresCloseError(t *testing.T) {
	closeSQLCloser(postgresErrCloser{})
}

const pgHelperStubDriver = "pghelper_stub_driver"

var _ = func() bool {
	sql.Register(pgHelperStubDriver, pgHelperDriver{})
	return true
}()

type pgHelperDriver struct{}

func (d pgHelperDriver) Open(name string) (driver.Conn, error) {
	// name is ignored; behavior controlled via global variables for tests.
	return &pgHelperConn{}, nil
}

type pgHelperConn struct{}

func (c *pgHelperConn) Prepare(query string) (driver.Stmt, error) {
	return &pgHelperStmt{}, nil
}

func (c *pgHelperConn) Close() error {
	return nil
}

func (c *pgHelperConn) Begin() (driver.Tx, error) {
	return &pgHelperTx{}, nil
}

// database/sql will use QueryerContext when available
func (c *pgHelperConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "ERR_QUERY"):
		return nil, errors.New("query failed")
	case strings.Contains(query, "ERR_ROWS"):
		return &pgHelperRows{mode: "errRows"}, nil
	case strings.Contains(query, "ZERO_COLS"):
		return &pgHelperRows{mode: "zeroCols"}, nil
	default:
		return &pgHelperRows{mode: "oneRow"}, nil
	}
}

type pgHelperStmt struct{}

func (s *pgHelperStmt) Close() error {
	return nil
}
func (s *pgHelperStmt) NumInput() int {
	return -1
}

func (s *pgHelperStmt) Exec(args []driver.Value) (driver.Result, error) {
	return pgHelperResult{rows: 1, last: 0, lastErr: driver.ErrSkip}, nil
}

func (s *pgHelperStmt) Query(args []driver.Value) (driver.Rows, error) {
	return &pgHelperRows{mode: "oneRow"}, nil
}

type pgHelperTx struct{}

func (t *pgHelperTx) Commit() error {
	return nil
}
func (t *pgHelperTx) Rollback() error {
	return nil
}

type pgHelperRows struct {
	mode    string
	emitted bool
}

func (r *pgHelperRows) Columns() []string {
	switch r.mode {
	case "zeroCols":
		return []string{}
	default:
		return []string{"id", "name", "bin", "nilv"}
	}
}
func (r *pgHelperRows) Close() error { return nil }

func (r *pgHelperRows) Next(dest []driver.Value) error {
	if r.mode == "errRows" {
		return errors.New("rows iteration error")
	}

	if r.emitted {
		return io.EOF
	}
	r.emitted = true

	// Produce one row with different types:
	// id -> int64, name -> string, bin -> []byte, nilv -> nil
	if len(dest) >= 4 {
		dest[0] = int64(42)
		dest[1] = "alice"
		dest[2] = []byte("blob")
		dest[3] = nil
	}
	return nil
}

type pgHelperResult struct {
	rows    int64
	last    int64
	lastErr error
}

func (r pgHelperResult) LastInsertId() (int64, error) {
	if r.lastErr != nil {
		return 0, r.lastErr
	}
	return r.last, nil
}

func (r pgHelperResult) RowsAffected() (int64, error) {
	return r.rows, nil
}

type resultOK struct {
	rows int64
	last int64
}

func (r resultOK) LastInsertId() (int64, error) {
	return r.last, nil
}

func (r resultOK) RowsAffected() (int64, error) {
	return r.rows, nil
}

type resultNoLastID struct {
	rows int64
}

func (r resultNoLastID) LastInsertId() (int64, error) {
	return 0, errors.New("unsupported")
}

func (r resultNoLastID) RowsAffected() (int64, error) {
	return r.rows, nil
}

type resultBadRowsAffected struct{}

func (r resultBadRowsAffected) LastInsertId() (int64, error) {
	return 0, nil
}

func (r resultBadRowsAffected) RowsAffected() (int64, error) {
	return 0, errors.New("rowsaffected failed")
}

func TestExecuteReadQueryContext_NilDBCtx_Error(t *testing.T) {
	_, err := ExecuteReadQuery(context.Background(), nil, ReadQueryInput{Query: "SELECT 1"})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestExecuteReadQueryContext_QueryError(t *testing.T) {
	dbctx := &DBContext{
		QueryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			return nil, errors.New("boom")
		},
	}
	_, err := ExecuteReadQuery(context.Background(), dbctx, ReadQueryInput{Query: "X"})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestExecuteReadQueryContext_Success_TypedValues_And_Capitalise(t *testing.T) {
	db, err := sql.Open(pgHelperStubDriver, "x")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("failed to close db: %v", err)
		}
	}()

	// Use Conn path: DBContext.QueryContext -> Conn.QueryContext -> sql.DB.QueryContext -> driver
	pdb := &PostgresDb{DB: db}
	dbctx := &DBContext{
		Conn: pdb,
	}

	out, err := ExecuteReadQuery(context.Background(), dbctx, ReadQueryInput{
		Query:             "SELECT_OK",
		Params:            []any{1},
		CapitaliseColumns: true,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 row, got %d", len(out))
	}

	row := out[0]
	if _, ok := row["Id"]; !ok {
		t.Fatalf("expected capitalised key Id")
	}
	if row["Id"].(int64) != 42 {
		t.Fatalf("expected id=42")
	}
	if row["Name"].(string) != "alice" {
		t.Fatalf("expected name=alice")
	}
	// normalizePgValue: []byte => string
	if row["Bin"].(string) != "blob" {
		t.Fatalf("expected bin normalized to string")
	}
	if row["Nilv"] != nil {
		t.Fatalf("expected nilv=nil")
	}
}

func TestExecuteReadQueryContext_RowsErr_Branch(t *testing.T) {
	db, err := sql.Open(pgHelperStubDriver, "x")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("failed to close db: %v", err)
		}
	}()

	dbctx := &DBContext{Conn: &PostgresDb{DB: db}}

	_, err = ExecuteReadQuery(context.Background(), dbctx, ReadQueryInput{
		Query: "SELECT_ERR_ROWS",
	})
	if err == nil {
		t.Fatalf("expected rows.Err error")
	}
}

func TestNormalizePgValue_Branches(t *testing.T) {
	if normalizePgValue(nil) != nil {
		t.Fatalf("nil should remain nil")
	}
	if got := normalizePgValue([]byte("x")).(string); got != "x" {
		t.Fatalf("[]byte should become string")
	}
	if got := normalizePgValue(int64(7)).(int64); got != 7 {
		t.Fatalf("other types should remain unchanged")
	}
}

func TestExecuteWriteQueryContext_NilDBCtx_Error(t *testing.T) {
	_, _, err := ExecuteWriteQuery(context.Background(), nil, "Q", []any{1})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestExecuteWriteQueryContext_ExecError(t *testing.T) {
	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return nil, errors.New("exec fail")
		},
	}
	_, _, err := ExecuteWriteQuery(context.Background(), dbctx, "Q", []any{1})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestExecuteWriteQueryContext_RowsAffectedError(t *testing.T) {
	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return resultBadRowsAffected{}, nil
		},
	}
	_, _, err := ExecuteWriteQuery(context.Background(), dbctx, "Q", []any{})
	if err == nil {
		t.Fatalf("expected rowsAffected error")
	}
}

func TestExecuteWriteQueryContext_LastInsertIdUnsupported_ReturnsZero(t *testing.T) {
	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return resultNoLastID{rows: 3}, nil
		},
	}
	ra, id, err := ExecuteWriteQuery(context.Background(), dbctx, "Q", []any{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ra != 3 || id != 0 {
		t.Fatalf("expected rows=3 lastID=0, got %d %d", ra, id)
	}
}

type miInner struct {
	A int `json:"a"`
}

type miRow struct {
	ID      int      `db:"id"`
	Name    string   `db:"name"`
	Skip    string   `db:"-"`
	Opt     string   `db:"opt,omitempty"`
	Inner   miInner  `db:"inner,marshaljson"`
	PtrOnly *miInner `db:"ptr,omitempty"`
}

func TestMultiInsertFromStructsArrayContext_Errors(t *testing.T) {
	_, err := MultiInsertFromStructsArray(context.Background(), &DBContext{}, "t", []miRow{})
	if err == nil {
		t.Fatalf("expected empty input error")
	}
	_, err = MultiInsertFromStructsArray(context.Background(), nil, "t", []miRow{{ID: 1}})
	if err == nil {
		t.Fatalf("expected nil dbctx error")
	}
}

func TestMultiInsertFromStructsArrayContext_GenerateAndExec_Success(t *testing.T) {
	var gotQuery string
	var gotArgs []any

	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			gotQuery = query
			gotArgs = append([]any{}, args...)
			return resultOK{rows: 2, last: 0}, nil
		},
	}

	rows := []miRow{
		{ID: 1, Name: "a", Opt: "", Inner: miInner{A: 7}}, // Opt empty => DEFAULT
		{ID: 2, Name: "b", Opt: "x", Inner: miInner{A: 9}},
	}

	ra, err := MultiInsertFromStructsArray(context.Background(), dbctx, "public.tbl", rows)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ra != 2 {
		t.Fatalf("expected rowsAffected=2")
	}

	// Query should contain quoted identifiers and placeholders
	if !strings.Contains(gotQuery, `INSERT INTO "public"."tbl"`) {
		t.Fatalf("expected schema-qualified quoted table, got: %s", gotQuery)
	}
	if !strings.Contains(gotQuery, `"id"`) || !strings.Contains(gotQuery, `"name"`) || !strings.Contains(gotQuery, `"opt"`) || !strings.Contains(gotQuery, `"inner"`) {
		t.Fatalf("expected quoted columns, got: %s", gotQuery)
	}

	// Expect DEFAULT present due to Opt empty + omitempty
	if !strings.Contains(gotQuery, "DEFAULT") {
		t.Fatalf("expected DEFAULT for omitempty field, got: %s", gotQuery)
	}

	// Inner should be marshaled into JSON string in args
	foundJSON := false
	for _, a := range gotArgs {
		if s, ok := a.(string); ok && strings.Contains(s, `"a":7`) {
			foundJSON = true
			break
		}
	}
	if !foundJSON {
		t.Fatalf("expected marshaled json arg for Inner")
	}
}

func TestGenerateMultiInsertQueriesFromStructArray_Errors(t *testing.T) {
	// bad table ident
	_, _, err := generateMultiInsertQueriesFromStructArray("bad;name", []miRow{{ID: 1}})
	if err == nil {
		t.Fatalf("expected safeIdent error")
	}

	// non-struct first row
	_, _, err = generateMultiInsertQueriesFromStructArray("t", []any{1})
	if err == nil {
		t.Fatalf("expected struct input error")
	}

	// non-struct element in slice (use []any with mixed types)
	_, _, err = generateMultiInsertQueriesFromStructArray("t", []any{miRow{ID: 1}, 2})
	if err == nil {
		t.Fatalf("expected struct input error for later row")
	}
}

type badJSON struct {
	F func() `json:"f"`
}
type rowInsert struct {
	ID   int     `db:"id"`
	Name string  `db:"name,omitempty"`
	Skip string  `db:"-"`
	JS   badJSON `db:"js,marshaljson"`
}

func TestInsertFromStructContext_Errors(t *testing.T) {
	_, _, err := InsertFromStruct(context.Background(), nil, "t", miRow{ID: 1})
	if err == nil {
		t.Fatalf("expected nil dbctx error")
	}

	_, _, err = InsertFromStruct(context.Background(), &DBContext{}, "bad;table", miRow{ID: 1})
	if err == nil {
		t.Fatalf("expected bad table ident error")
	}

	_, _, err = InsertFromStruct(context.Background(), &DBContext{}, "t", 123)
	if err == nil {
		t.Fatalf("expected struct input error")
	}
}

func TestInsertFromStructContext_NoInsertableFields_Error(t *testing.T) {
	type onlySkip struct {
		X string `db:"-"`
	}
	_, _, err := InsertFromStruct(context.Background(), &DBContext{}, "t", onlySkip{X: "x"})
	if err == nil {
		t.Fatalf("expected no insertable fields error")
	}
}

func TestInsertFromStructContext_MarshalJSONError(t *testing.T) {
	// badJSON contains func => json.Marshal fails
	dbctx := &DBContext{}
	_, _, err := InsertFromStruct(context.Background(), dbctx, "t", rowInsert{ID: 1, JS: badJSON{}})
	if err == nil {
		t.Fatalf("expected json marshal error")
	}
}

func TestInsertFromStructContext_Success_ExecAndLastIdFallback(t *testing.T) {
	var gotQuery string
	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			gotQuery = query
			// Return unsupported LastInsertId to hit fallback=0
			return resultNoLastID{rows: 1}, nil
		},
	}
	ra, id, err := InsertFromStruct(context.Background(), dbctx, "t", miRow{ID: 1, Name: "", Inner: miInner{A: 1}})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ra != 1 || id != 0 {
		t.Fatalf("expected rows=1 id=0, got %d %d", ra, id)
	}
	if !strings.Contains(gotQuery, `INSERT INTO "t"`) {
		t.Fatalf("expected insert query to include quoted table")
	}
}

func TestInsertFromMapContext_Errors(t *testing.T) {
	_, _, err := InsertFromMap(context.Background(), nil, "t", map[string]any{"a": 1})
	if err == nil {
		t.Fatalf("expected nil dbctx error")
	}
	_, _, err = InsertFromMap(context.Background(), &DBContext{}, "t", map[string]any{})
	if err == nil {
		t.Fatalf("expected empty map error")
	}
	_, _, err = InsertFromMap(context.Background(), &DBContext{}, "bad;table", map[string]any{"a": 1})
	if err == nil {
		t.Fatalf("expected bad table ident")
	}
	_, _, err = InsertFromMap(context.Background(), &DBContext{}, "t", map[string]any{"bad;col": 1})
	if err == nil {
		t.Fatalf("expected bad column ident")
	}
}

func TestInsertFromMapContext_Success_StableOrdering(t *testing.T) {
	var gotQuery string
	var gotArgs []any
	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			gotQuery = query
			gotArgs = append([]any{}, args...)
			return resultOK{rows: 1, last: 99}, nil
		},
	}

	ra, id, err := InsertFromMap(context.Background(), dbctx, "t", map[string]any{
		"b": 2,
		"a": 1,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ra != 1 || id != 99 {
		t.Fatalf("expected rows=1 id=99")
	}
	// Ensure stable sort a then b => placeholders $1,$2 with args [1,2]
	if !strings.Contains(gotQuery, `"a", "b"`) {
		t.Fatalf("expected sorted columns in query, got: %s", gotQuery)
	}
	if len(gotArgs) != 2 || gotArgs[0].(int) != 1 || gotArgs[1].(int) != 2 {
		t.Fatalf("expected args ordered [1,2], got %#v", gotArgs)
	}
}

func TestUpdateFromMapContext_Errors(t *testing.T) {
	_, err := UpdateFromMap(context.Background(), nil, "t", map[string]any{"a": 1}, "id=$1")
	if err == nil {
		t.Fatalf("expected nil dbctx error")
	}
	_, err = UpdateFromMap(context.Background(), &DBContext{}, "t", map[string]any{}, "id=$1")
	if err == nil {
		t.Fatalf("expected empty update map error")
	}
	_, err = UpdateFromMap(context.Background(), &DBContext{}, "bad;table", map[string]any{"a": 1}, "id=$1")
	if err == nil {
		t.Fatalf("expected bad table ident")
	}
	_, err = UpdateFromMap(context.Background(), &DBContext{}, "t", map[string]any{"bad;col": 1}, "id=$1")
	if err == nil {
		t.Fatalf("expected bad column ident")
	}

	// mismatch '?' count vs params triggers error
	_, err = UpdateFromMap(context.Background(), &DBContext{}, "t", map[string]any{"a": 1}, "id=? AND x=?", 1)
	if err == nil {
		t.Fatalf("expected where placeholder mismatch error")
	}
}

func TestUpdateFromMapContext_Success_ConvertsQuestionMarks(t *testing.T) {
	var gotQuery string
	var gotArgs []any

	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			gotQuery = query
			gotArgs = append([]any{}, args...)
			return resultOK{rows: 5, last: 0}, nil
		},
	}

	ra, err := UpdateFromMap(context.Background(), dbctx, "t", map[string]any{
		"b": 2,
		"a": 1,
	}, "id=? AND org=?", 10, "o1")

	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ra != 5 {
		t.Fatalf("expected rowsAffected=5")
	}
	// SET a=$1,b=$2 (sorted), WHERE id=$3 AND org=$4
	if !strings.Contains(gotQuery, `SET "a" = $1, "b" = $2`) {
		t.Fatalf("expected sorted SET placeholders, got: %s", gotQuery)
	}
	if !strings.Contains(gotQuery, `WHERE id=$3 AND org=$4`) {
		t.Fatalf("expected converted where placeholders, got: %s", gotQuery)
	}
	if len(gotArgs) != 4 {
		t.Fatalf("expected 4 args, got %d", len(gotArgs))
	}
}

func TestDeleteByPrimaryKeyContext_Errors(t *testing.T) {
	_, err := DeleteByPrimaryKey(context.Background(), nil, "t", "id", 1)
	if err == nil {
		t.Fatalf("expected nil dbctx error")
	}
	_, err = DeleteByPrimaryKey(context.Background(), &DBContext{}, "bad;table", "id", 1)
	if err == nil {
		t.Fatalf("expected bad table ident error")
	}
	_, err = DeleteByPrimaryKey(context.Background(), &DBContext{}, "t", "bad;col", 1)
	if err == nil {
		t.Fatalf("expected bad col ident error")
	}
}

func TestDeleteByPrimaryKeyContext_Success(t *testing.T) {
	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			if !strings.Contains(query, `DELETE FROM "t" WHERE "id" = $1`) {
				t.Fatalf("unexpected query: %s", query)
			}
			if len(args) != 1 || args[0].(int) != 7 {
				t.Fatalf("unexpected args: %#v", args)
			}
			return resultOK{rows: 1, last: 0}, nil
		},
	}
	ra, err := DeleteByPrimaryKey(context.Background(), dbctx, "t", "id", 7)
	if err != nil || ra != 1 {
		t.Fatalf("expected success, got ra=%d err=%v", ra, err)
	}
}

func TestSoftDeleteByPrimaryKeyContext_Errors(t *testing.T) {
	_, err := SoftDeleteByPrimaryKey(context.Background(), nil, "t", "del", "id", 1)
	if err == nil {
		t.Fatalf("expected nil dbctx error")
	}
	_, err = SoftDeleteByPrimaryKey(context.Background(), &DBContext{}, "bad;table", "del", "id", 1)
	if err == nil {
		t.Fatalf("expected bad table ident")
	}
	_, err = SoftDeleteByPrimaryKey(context.Background(), &DBContext{}, "t", "bad;col", "id", 1)
	if err == nil {
		t.Fatalf("expected bad delete col ident")
	}
	_, err = SoftDeleteByPrimaryKey(context.Background(), &DBContext{}, "t", "del", "bad;col", 1)
	if err == nil {
		t.Fatalf("expected bad pk col ident")
	}
}

func TestSoftDeleteByPrimaryKeyContext_Success(t *testing.T) {
	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			if !strings.Contains(query, `UPDATE "t" SET "is_deleted" = $1 WHERE "id" = $2`) {
				t.Fatalf("unexpected query: %s", query)
			}
			if len(args) != 2 || args[0].(bool) != true || args[1].(int) != 9 {
				t.Fatalf("unexpected args: %#v", args)
			}
			return resultOK{rows: 3, last: 0}, nil
		},
	}
	ra, err := SoftDeleteByPrimaryKey(context.Background(), dbctx, "t", "is_deleted", "id", 9)
	if err != nil || ra != 3 {
		t.Fatalf("expected success ra=3, got ra=%d err=%v", ra, err)
	}
}

func TestGetParameterizedInClause(t *testing.T) {
	clause, mp := GetParameterizedInClause("id", []int{10, 20, 30})
	if clause != ":id1,:id2,:id3" {
		t.Fatalf("unexpected clause: %s", clause)
	}
	if mp[":id1"].(int) != 10 || mp[":id2"].(int) != 20 || mp[":id3"].(int) != 30 {
		t.Fatalf("unexpected map: %#v", mp)
	}
}

func TestParseDollarTag(t *testing.T) {
	if tag, ok := parseDollarTag("$$abc"); !ok || tag != "$$" {
		t.Fatalf("expected $$")
	}
	if tag, ok := parseDollarTag("$tag$ rest"); !ok || tag != "$tag$" {
		t.Fatalf("expected $tag$")
	}
	if _, ok := parseDollarTag("$bad-tag$"); ok {
		t.Fatalf("expected invalid tag due to '-'")
	}
	if _, ok := parseDollarTag("nope"); ok {
		t.Fatalf("expected false")
	}
}

func TestConvertQueryAndNamedParams_RobustParsing(t *testing.T) {
	q := `
SELECT
  ':not' as s1,
  $$:also_not$$ as s2,
  col::int as casted,
  x
FROM t
WHERE id = :id AND org = :org AND id2 = :id AND lone=: AND ::int=col::int
`
	outQ, args := ConvertQueryAndNamedParams(q, map[string]any{
		":id":  7,
		":org": "o1",
	})

	// :id appears twice => reused same $1
	if strings.Count(outQ, "$1") != 2 {
		t.Fatalf("expected $1 reused twice, outQ=%s", outQ)
	}
	if !strings.Contains(outQ, "org = $2") {
		t.Fatalf("expected org = $2")
	}
	// ensure casts ::int remain (not treated as param)
	if strings.Contains(outQ, "$") && strings.Contains(outQ, "::int") == false {
		t.Fatalf("expected cast preserved")
	}
	// ":" with no name should remain literal
	if !strings.Contains(outQ, "lone=:") {
		t.Fatalf("expected lone=: preserved")
	}

	if len(args) != 2 || args[0].(int) != 7 || args[1].(string) != "o1" {
		t.Fatalf("unexpected ordered args: %#v", args)
	}
}

func TestHashKey_Deterministic_OrderIndependence(t *testing.T) {
	h1, err := HashKey("q", map[string]any{"b": 2, "a": 1})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	h2, err := HashKey("q", map[string]any{"a": 1, "b": 2})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if h1 != h2 {
		t.Fatalf("expected deterministic hash, got %s vs %s", h1, h2)
	}
	// ensure it is base64 url encoded (rough check)
	_, decErr := base64.URLEncoding.DecodeString(h1)
	if decErr != nil {
		t.Fatalf("expected base64 decodable, err=%v", decErr)
	}
}

func TestHashKey_WriteError(t *testing.T) {
	orig := hashFprintfHook
	defer func() { hashFprintfHook = orig }()

	hashFprintfHook = func(io.Writer, string, ...any) (int, error) {
		return 0, errors.New("write failed")
	}

	key, err := HashKey("q", map[string]any{"a": 1})
	if err == nil {
		t.Fatalf("expected write error")
	}
	if key != "" {
		t.Fatalf("expected empty key on error, got %q", key)
	}
}

func TestSafeIdent_Branches(t *testing.T) {
	if _, err := safeIdent(""); err == nil {
		t.Fatalf("expected empty identifier error")
	}
	if _, err := safeIdent("a;drop"); err == nil {
		t.Fatalf("expected invalid identifier error")
	}
	if _, err := safeIdent("a..b"); err == nil {
		t.Fatalf("expected invalid identifier error due to empty segment")
	}
	got, err := safeIdent("public.tbl")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != `"public"."tbl"` {
		t.Fatalf("unexpected quoted ident: %s", got)
	}
}

// -----------------------------------------------------
// Tests: convertWhereParamsToPg
// -----------------------------------------------------

func TestConvertWhereParamsToPg_Branches(t *testing.T) {
	// no '?' => return as-is
	out, vals, err := convertWhereParamsToPg("id=$1", 2, 10)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "id=$1" || len(vals) != 1 || vals[0].(int) != 10 {
		t.Fatalf("unexpected result")
	}

	// mismatch
	_, _, err = convertWhereParamsToPg("id=? AND x=?", 3, 1)
	if err == nil {
		t.Fatalf("expected mismatch error")
	}

	// convert starting at $5
	out, vals, err = convertWhereParamsToPg("id=? AND org=? AND z=?", 5, 1, "o", 9)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "id=$5 AND org=$6 AND z=$7" {
		t.Fatalf("unexpected where: %s", out)
	}
	if !reflect.DeepEqual(vals, []any{1, "o", 9}) {
		t.Fatalf("unexpected vals: %#v", vals)
	}
}

func TestNowUTC(t *testing.T) {
	n := nowUTC()
	if n.Location() != time.UTC {
		t.Fatalf("expected UTC time")
	}
}

func TestExecuteReadQueryContext_ColumnsAndScanHookErrors(t *testing.T) {
	db, err := sql.Open(pgHelperStubDriver, "x")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("failed to close db: %v", err)
		}
	}()

	dbctx := &DBContext{Conn: &PostgresDb{DB: db}}

	origCols := getReadColumnsHook
	origScan := scanReadRowHook
	defer func() {
		getReadColumnsHook = origCols
		scanReadRowHook = origScan
	}()

	getReadColumnsHook = func(rows *sql.Rows) ([]string, error) {
		return nil, errors.New("columns failed")
	}
	if _, err := ExecuteReadQuery(context.Background(), dbctx, ReadQueryInput{Query: "SELECT_OK"}); err == nil {
		t.Fatalf("expected columns error")
	}

	getReadColumnsHook = origCols
	scanReadRowHook = func(rows *sql.Rows, dest ...any) error {
		return errors.New("scan failed")
	}
	if _, err := ExecuteReadQuery(context.Background(), dbctx, ReadQueryInput{Query: "SELECT_OK"}); err == nil {
		t.Fatalf("expected scan error")
	}
}

func TestMultiInsertFromStructsArrayContext_ErrorBranches(t *testing.T) {
	type row struct {
		ID int `db:"id"`
	}

	_, err := MultiInsertFromStructsArray(context.Background(), &DBContext{}, "bad;table", []row{{ID: 1}})
	if err == nil {
		t.Fatalf("expected generate error")
	}

	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return nil, errors.New("exec fail")
		},
	}
	_, err = MultiInsertFromStructsArray(context.Background(), dbctx, "t", []row{{ID: 1}})
	if err == nil {
		t.Fatalf("expected exec error")
	}

	dbctx = &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return resultBadRowsAffected{}, nil
		},
	}
	_, err = MultiInsertFromStructsArray(context.Background(), dbctx, "t", []row{{ID: 1}})
	if err == nil {
		t.Fatalf("expected rows affected error")
	}
}

func TestGenerateMultiInsertQueriesFromStructArray_PointerAndTags(t *testing.T) {
	type ptrRow struct {
		ID   int `db:"id"`
		Name string
		Dup1 int `db:"dup"`
		Dup2 int `db:"dup"`
	}
	r1 := &ptrRow{ID: 1, Name: "a", Dup1: 2, Dup2: 3}
	r2 := &ptrRow{ID: 2, Name: "b", Dup1: 4, Dup2: 5}

	q, vals, err := generateMultiInsertQueriesFromStructArray("t", []*ptrRow{r1, r2})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !strings.Contains(q, `"id"`) || !strings.Contains(q, `"dup"`) {
		t.Fatalf("unexpected query: %s", q)
	}
	if len(vals) == 0 {
		t.Fatalf("expected values")
	}
}

func TestGenerateMultiInsertQueriesFromStructArray_SafeIdentAndMarshalErrors(t *testing.T) {
	type badCol struct {
		ID int `db:"bad;col"`
	}
	if _, _, err := generateMultiInsertQueriesFromStructArray("t", []badCol{{ID: 1}}); err == nil {
		t.Fatalf("expected bad column safeIdent error")
	}

	type badMarshal struct {
		ID int     `db:"id"`
		JS badJSON `db:"js,marshaljson"`
	}
	if _, _, err := generateMultiInsertQueriesFromStructArray("t", []badMarshal{{ID: 1}}); err == nil {
		t.Fatalf("expected marshaljson error")
	}
}

func TestInsertFromStruct_Wrapper_And_PointerPath(t *testing.T) {
	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return resultOK{rows: 1, last: 11}, nil
		},
	}
	type row struct {
		ID   int `db:"id"`
		Name string
	}
	ra, id, err := InsertFromStruct(context.Background(), dbctx, "t", &row{ID: 1, Name: "n"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ra != 1 || id != 11 {
		t.Fatalf("unexpected result ra=%d id=%d", ra, id)
	}
}

func TestInsertFromStructContext_ErrorBranches(t *testing.T) {
	type badCol struct {
		ID int `db:"bad;col"`
	}
	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return nil, errors.New("exec fail")
		},
	}
	_, _, err := InsertFromStruct(context.Background(), &DBContext{}, "t", badCol{ID: 1})
	if err == nil {
		t.Fatalf("expected safeIdent error")
	}
	_, _, err = InsertFromStruct(context.Background(), dbctx, "t", miRow{ID: 1})
	if err == nil {
		t.Fatalf("expected exec error")
	}
}

func TestInsertFromMapContext_ExecAndLastInsertFallback(t *testing.T) {
	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return nil, errors.New("exec fail")
		},
	}
	if _, _, err := InsertFromMap(context.Background(), dbctx, "t", map[string]any{"a": 1}); err == nil {
		t.Fatalf("expected exec error")
	}

	dbctx = &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return resultNoLastID{rows: 2}, nil
		},
	}
	ra, id, err := InsertFromMap(context.Background(), dbctx, "t", map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ra != 2 || id != 0 {
		t.Fatalf("expected fallback id=0, got ra=%d id=%d", ra, id)
	}
}

func TestUpdateDeleteSoftDelete_ExecErrors(t *testing.T) {
	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return nil, errors.New("exec fail")
		},
	}
	if _, err := UpdateFromMap(context.Background(), dbctx, "t", map[string]any{"a": 1}, "id=$1", 1); err == nil {
		t.Fatalf("expected update exec error")
	}
	if _, err := DeleteByPrimaryKey(context.Background(), dbctx, "t", "id", 1); err == nil {
		t.Fatalf("expected delete exec error")
	}
	if _, err := SoftDeleteByPrimaryKey(context.Background(), dbctx, "t", "is_deleted", "id", 1); err == nil {
		t.Fatalf("expected soft delete exec error")
	}
}

func TestConvertQueryAndNamedParams_EscapedSingleQuote(t *testing.T) {
	q := "SELECT ':x', 'it''s :still_string', id FROM t WHERE id=:id"
	outQ, args := ConvertQueryAndNamedParams(q, map[string]any{":id": 9})
	if !strings.Contains(outQ, "'it''s :still_string'") {
		t.Fatalf("escaped quote string handling failed: %s", outQ)
	}
	if len(args) != 1 || args[0].(int) != 9 {
		t.Fatalf("unexpected args: %#v", args)
	}
}

func TestConvertQueryAndNamedParams_DollarQuoteWithStrayDollar(t *testing.T) {
	q := "SELECT $tag$body $ still :ignored$tag$ FROM t WHERE id=:id"
	outQ, args := ConvertQueryAndNamedParams(q, map[string]any{":id": 11})

	if !strings.Contains(outQ, "$tag$body $ still :ignored$tag$") {
		t.Fatalf("expected dollar quoted body to remain unchanged: %s", outQ)
	}
	if !strings.Contains(outQ, "id=$1") {
		t.Fatalf("expected named param to be converted: %s", outQ)
	}
	if len(args) != 1 || args[0].(int) != 11 {
		t.Fatalf("unexpected args: %#v", args)
	}
}

func TestConvertQueryAndNamedParams_DollarInsideSingleQuote(t *testing.T) {
	q := "SELECT '$not_dollar_tag :ignored' FROM t WHERE id=:id"
	outQ, args := ConvertQueryAndNamedParams(q, map[string]any{":id": 12})

	if !strings.Contains(outQ, "'$not_dollar_tag :ignored'") {
		t.Fatalf("expected single-quoted dollar text to remain unchanged: %s", outQ)
	}
	if !strings.Contains(outQ, "id=$1") {
		t.Fatalf("expected named param to be converted: %s", outQ)
	}
	if len(args) != 1 || args[0].(int) != 12 {
		t.Fatalf("unexpected args: %#v", args)
	}
}

func TestConvertQueryAndNamedParams_InvalidDollarTag(t *testing.T) {
	q := "SELECT price$invalid FROM t WHERE id=:id"
	outQ, args := ConvertQueryAndNamedParams(q, map[string]any{":id": 13})

	if !strings.Contains(outQ, "price$invalid") {
		t.Fatalf("expected invalid dollar tag text to remain unchanged: %s", outQ)
	}
	if !strings.Contains(outQ, "id=$1") {
		t.Fatalf("expected named param to be converted: %s", outQ)
	}
	if len(args) != 1 || args[0].(int) != 13 {
		t.Fatalf("unexpected args: %#v", args)
	}
}

func TestParseDollarTag_Unterminated(t *testing.T) {
	if _, ok := parseDollarTag("$unterminated"); ok {
		t.Fatalf("expected false for unterminated dollar tag")
	}
}

func TestExecuteReadQuery_Wrapper_CallsContextVariant(t *testing.T) {
	db, err := sql.Open(pgHelperStubDriver, "x")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("failed to close db: %v", err)
		}
	}()

	dbctx := &DBContext{Conn: &PostgresDb{DB: db}}

	out, err := ExecuteReadQuery(context.Background(), dbctx, ReadQueryInput{
		Query:             "SELECT_OK",
		Params:            []any{1},
		CapitaliseColumns: false,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 row, got %d", len(out))
	}
	if out[0]["id"].(int64) != 42 {
		t.Fatalf("expected id=42")
	}
}

func TestExecuteWriteQuery_Wrapper_CallsContextVariant(t *testing.T) {
	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			return resultNoLastID{rows: 7}, nil
		},
	}

	ra, id, err := ExecuteWriteQuery(context.Background(), dbctx, "UPDATE x", []any{1})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ra != 7 || id != 0 {
		t.Fatalf("expected rows=7 lastID=0, got %d %d", ra, id)
	}
}

func TestInsertFromMap_Wrapper_CallsContextVariant(t *testing.T) {
	var gotQuery string
	var gotArgs []any

	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			gotQuery = query
			gotArgs = append([]any{}, args...)
			return resultOK{rows: 1, last: 101}, nil
		},
	}

	ra, id, err := InsertFromMap(context.Background(), dbctx, "t", map[string]any{"b": 2, "a": 1})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ra != 1 || id != 101 {
		t.Fatalf("expected rows=1 id=101")
	}
	if !strings.Contains(gotQuery, `INSERT INTO "t"`) {
		t.Fatalf("unexpected query: %s", gotQuery)
	}
	if len(gotArgs) != 2 || gotArgs[0].(int) != 1 || gotArgs[1].(int) != 2 {
		t.Fatalf("unexpected args order: %#v", gotArgs)
	}
}

func TestUpdateFromMap_Wrapper_CallsContextVariant(t *testing.T) {
	var gotQuery string
	var gotArgs []any

	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			gotQuery = query
			gotArgs = append([]any{}, args...)
			return resultOK{rows: 3, last: 0}, nil
		},
	}

	ra, err := UpdateFromMap(context.Background(), dbctx, "t", map[string]any{"b": 2, "a": 1}, "id=?", 99)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ra != 3 {
		t.Fatalf("expected rows=3, got %d", ra)
	}
	// a then b => $1,$2 ; where starts at $3
	if !strings.Contains(gotQuery, `SET "a" = $1, "b" = $2 WHERE id=$3`) {
		t.Fatalf("unexpected query: %s", gotQuery)
	}
	if len(gotArgs) != 3 || gotArgs[2].(int) != 99 {
		t.Fatalf("unexpected args: %#v", gotArgs)
	}
}

func TestDeleteByPrimaryKey_Wrapper_CallsContextVariant(t *testing.T) {
	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			if !strings.Contains(query, `DELETE FROM "t" WHERE "id" = $1`) {
				t.Fatalf("unexpected query: %s", query)
			}
			return resultOK{rows: 5, last: 0}, nil
		},
	}

	ra, err := DeleteByPrimaryKey(context.Background(), dbctx, "t", "id", 123)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ra != 5 {
		t.Fatalf("expected rows=5, got %d", ra)
	}
}

func TestSoftDeleteByPrimaryKey_Wrapper_CallsContextVariant(t *testing.T) {
	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			if !strings.Contains(query, `UPDATE "t" SET "is_deleted" = $1 WHERE "id" = $2`) {
				t.Fatalf("unexpected query: %s", query)
			}
			if len(args) != 2 || args[0].(bool) != true || args[1].(int) != 9 {
				t.Fatalf("unexpected args: %#v", args)
			}
			return resultOK{rows: 2, last: 0}, nil
		},
	}

	ra, err := SoftDeleteByPrimaryKey(context.Background(), dbctx, "t", "is_deleted", "id", 9)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ra != 2 {
		t.Fatalf("expected rows=2, got %d", ra)
	}
}

func TestMultiInsertFromStructsArray_Wrapper_CallsContextVariant(t *testing.T) {
	type meta struct {
		X int `json:"x"`
	}

	type row struct {
		ID   int    `db:"id,omitempty"` // omitempty => DEFAULT when zero
		Name string `db:"name"`
		M    meta   `db:"m,marshaljson"` // struct -> json string
	}

	var gotQuery string
	var gotArgs []any

	dbctx := &DBContext{
		ExecFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			gotQuery = query
			gotArgs = append([]any{}, args...)
			return resultOK{rows: 2, last: 0}, nil
		},
	}

	ra, err := MultiInsertFromStructsArray(context.Background(), dbctx, "t", []row{
		{ID: 0, Name: "a", M: meta{X: 1}}, // ID => DEFAULT
		{ID: 7, Name: "b", M: meta{X: 2}}, // ID => $N
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ra != 2 {
		t.Fatalf("expected rows=2, got %d", ra)
	}

	if !strings.Contains(gotQuery, `INSERT INTO "t"`) {
		t.Fatalf("unexpected query: %s", gotQuery)
	}

	gotQuery = strings.Join(strings.Fields(strings.TrimSpace(gotQuery)), "") // normalize spaces

	expectedQuery := `(DEFAULT,$1,$2),($3,$4,$5)`
	if !strings.Contains(gotQuery, expectedQuery) {
		t.Fatalf("unexpected placeholders/defaults in query: %s; expected: %s", gotQuery, expectedQuery)
	}

	if len(gotArgs) != 5 {
		t.Fatalf("expected 5 args, got %d: %#v", len(gotArgs), gotArgs)
	}
	if gotArgs[0].(string) != "a" || gotArgs[3].(string) != "b" {
		t.Fatalf("unexpected args ordering: %#v", gotArgs)
	}
	if gotArgs[1].(string) == "" || gotArgs[4].(string) == "" {
		t.Fatalf("expected json strings for marshaljson: %#v", gotArgs)
	}
}
