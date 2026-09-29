package mysql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockTx struct {
	mock.Mock
}

func (m *MockTx) Exec(query string, args ...any) (sql.Result, error) {
	argsSlice := m.Called(query, args)
	return argsSlice.Get(0).(sql.Result), argsSlice.Error(1)
}

func (m *MockTx) Prepare(query string) (*sql.Stmt, error) {
	argsSlice := m.Called(query)
	return argsSlice.Get(0).(*sql.Stmt), argsSlice.Error(1)
}

func (m *MockTx) Query(query string, args ...any) (*sql.Rows, error) {
	argsSlice := m.Called(query, args)
	return argsSlice.Get(0).(*sql.Rows), argsSlice.Error(1)
}

type MockTxContext struct {
	MockTx
}

func (m *MockTxContext) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	argsSlice := m.Called(ctx, query, args)
	return argsSlice.Get(0).(sql.Result), argsSlice.Error(1)
}

func (m *MockTxContext) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	argsSlice := m.Called(ctx, query)
	return argsSlice.Get(0).(*sql.Stmt), argsSlice.Error(1)
}

func (m *MockTxContext) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	argsSlice := m.Called(ctx, query, args)
	return argsSlice.Get(0).(*sql.Rows), argsSlice.Error(1)
}

type MockResult struct {
	mock.Mock
}

func (m *MockResult) LastInsertId() (int64, error) { return 1, nil }
func (m *MockResult) RowsAffected() (int64, error) { return 1, nil }

func TestDBContext_Exec(t *testing.T) {
	mockResult := new(MockResult)

	// Case 1: ExecFn override
	ctx := &DBContext{
		ExecFn: func(query string, args ...any) (sql.Result, error) {
			return mockResult, nil
		},
	}
	res, err := ctx.Exec(context.Background(), "fake", 1)
	assert.NoError(t, err)
	assert.Equal(t, mockResult, res)
}

func TestDBContext_ContextOverridesAndCancellation(t *testing.T) {
	mockResult := new(MockResult)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("ExecContextFnReceivesContext", func(t *testing.T) {
		receivedCanceledContext := false
		dbctx := &DBContext{
			ExecContextFn: func(c context.Context, query string, args ...any) (sql.Result, error) {
				receivedCanceledContext = c.Err() == context.Canceled
				assert.Equal(t, "UPDATE x SET y = ?", query)
				assert.Equal(t, []any{1}, args)
				return mockResult, c.Err()
			},
		}

		res, err := dbctx.Exec(ctx, "UPDATE x SET y = ?", 1)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, mockResult, res)
		assert.True(t, receivedCanceledContext)
	})

	t.Run("QueryContextFnReceivesContext", func(t *testing.T) {
		receivedCanceledContext := false
		dbctx := &DBContext{
			QueryContextFn: func(c context.Context, query string, args ...any) (*sql.Rows, error) {
				receivedCanceledContext = c.Err() == context.Canceled
				assert.Equal(t, "SELECT * FROM x WHERE id = ?", query)
				assert.Equal(t, []any{7}, args)
				return nil, c.Err()
			},
		}

		rows, err := dbctx.Query(ctx, "SELECT * FROM x WHERE id = ?", 7)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, rows)
		assert.True(t, receivedCanceledContext)
	})
}

func TestDBContext_PublicWrappersAndTxContextBranches(t *testing.T) {
	mockResult := new(MockResult)
	stmt := new(sql.Stmt)
	rows := new(sql.Rows)

	t.Run("ExecFallsBackToContextPath", func(t *testing.T) {
		tx := new(MockTx)
		tx.On("Exec", "exec-wrapper", []any{1}).Return(mockResult, nil)

		dbctx := &DBContext{Tx: tx}
		res, err := dbctx.Exec(context.Background(), "exec-wrapper", 1)
		assert.NoError(t, err)
		assert.Equal(t, mockResult, res)
		tx.AssertExpectations(t)
	})

	t.Run("PrepareFallsBackToContextPath", func(t *testing.T) {
		tx := new(MockTx)
		tx.On("Prepare", "prepare-wrapper").Return(stmt, nil)

		dbctx := &DBContext{Tx: tx}
		got, err := dbctx.Prepare(context.Background(), "prepare-wrapper")
		assert.NoError(t, err)
		assert.Equal(t, stmt, got)
		tx.AssertExpectations(t)
	})

	t.Run("PrepareContextFnReceivesContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		receivedCanceledContext := false

		dbctx := &DBContext{
			PrepareContextFn: func(c context.Context, query string) (*sql.Stmt, error) {
				receivedCanceledContext = c.Err() == context.Canceled
				assert.Equal(t, "prepare-fn", query)
				return stmt, c.Err()
			},
		}

		got, err := dbctx.Prepare(ctx, "prepare-fn")
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, stmt, got)
		assert.True(t, receivedCanceledContext)
	})

	t.Run("TxContextBranchesAcceptNilContext", func(t *testing.T) {
		tx := new(MockTxContext)
		tx.On("ExecContext", mock.Anything, "exec-context", []any{1}).Return(mockResult, nil)
		tx.On("PrepareContext", mock.Anything, "prepare-context").Return(stmt, nil)
		tx.On("QueryContext", mock.Anything, "query-context", []any{2}).Return(rows, nil)

		dbctx := &DBContext{Tx: tx}
		var nilCtx context.Context

		res, err := dbctx.Exec(nilCtx, "exec-context", 1)
		assert.NoError(t, err)
		assert.Equal(t, mockResult, res)

		gotStmt, err := dbctx.Prepare(nilCtx, "prepare-context")
		assert.NoError(t, err)
		assert.Equal(t, stmt, gotStmt)

		gotRows, err := dbctx.Query(nilCtx, "query-context", 2)
		assert.NoError(t, err)
		assert.Equal(t, rows, gotRows)

		tx.AssertExpectations(t)
	})
}

func TestDBContext_exec(t *testing.T) {
	mockResult := new(MockResult)

	t.Run("UsingTx", func(t *testing.T) {
		tx := new(MockTx)
		tx.On("Exec", "q1", mock.Anything).Return(mockResult, nil)
		ctx := &DBContext{Tx: tx}
		res, err := ctx.exec(context.Background(), "q1", 1)
		assert.NoError(t, err)
		assert.Equal(t, mockResult, res)
	})

	t.Run("UsingConn", func(t *testing.T) {
		mockConn := new(MockedMySqlDb)
		mockConn.On("Exec", "q2", mock.Anything).Return(mockResult, nil)
		ctx := &DBContext{Conn: mockConn}
		res, err := ctx.exec(context.Background(), "q2", 1)
		assert.NoError(t, err)
		assert.Equal(t, mockResult, res)
	})

	t.Run("UsingCluster", func(t *testing.T) {
		SetConnectionConfig("test-cluster-x", &ConnectionConfig{Host: "x", DbName: "y"})
		mockDb := new(MockedMySqlDb)
		mockDb.On("Ping").Return(nil)
		mockDb.On("Exec", "q3", mock.Anything).Return(mockResult, nil)
		mockConnector := new(MockedMySqlDbConnector)
		mockConnector.On("Open", "mysql", mock.Anything).Return(mockDb, nil)

		original := helperMySqlConnector
		helperMySqlConnector = mockConnector
		t.Cleanup(func() {
			helperMySqlConnector = original
		})

		ctx := &DBContext{Cluster: "test-cluster-x"}
		res, err := ctx.exec(context.Background(), "q3", 1)
		assert.NoError(t, err)
		assert.Equal(t, mockResult, res)
	})

	t.Run("InvalidCluster", func(t *testing.T) {
		ctx := &DBContext{Cluster: "no-cluster"}
		res, err := ctx.exec(context.Background(), "bad", 1)
		assert.Error(t, err)
		assert.Nil(t, res)
	})
}

func TestDBContext_Prepare(t *testing.T) {
	stmt := new(sql.Stmt)
	ctx := &DBContext{
		PrepareFn: func(query string) (*sql.Stmt, error) {
			return stmt, nil
		},
	}
	s, err := ctx.Prepare(context.Background(), "SELECT")
	assert.NoError(t, err)
	assert.Equal(t, stmt, s)
}

func TestDBContext_prepare(t *testing.T) {
	stmt := new(sql.Stmt)

	t.Run("UsingTx", func(t *testing.T) {
		tx := new(MockTx)
		tx.On("Prepare", "q1").Return(stmt, nil)
		ctx := &DBContext{Tx: tx}
		s, err := ctx.prepare(context.Background(), "q1")
		assert.NoError(t, err)
		assert.Equal(t, stmt, s)
	})

	t.Run("UsingConn", func(t *testing.T) {
		conn := new(MockedMySqlDb)
		conn.On("Prepare", "q2").Return(stmt, nil)
		ctx := &DBContext{Conn: conn}
		s, err := ctx.prepare(context.Background(), "q2")
		assert.NoError(t, err)
		assert.Equal(t, stmt, s)
	})

	t.Run("UsingCluster", func(t *testing.T) {
		SetConnectionConfig("c", &ConnectionConfig{Host: "x", DbName: "y"})
		db := new(MockedMySqlDb)
		db.On("Ping").Return(nil)
		db.On("Prepare", "q3").Return(stmt, nil)
		conn := new(MockedMySqlDbConnector)
		conn.On("Open", "mysql", mock.Anything).Return(db, nil)

		original := helperMySqlConnector
		helperMySqlConnector = conn
		t.Cleanup(func() {
			helperMySqlConnector = original
		})

		ctx := &DBContext{Cluster: "c"}
		s, err := ctx.prepare(context.Background(), "q3")
		assert.NoError(t, err)
		assert.Equal(t, stmt, s)
	})

	t.Run("InvalidCluster", func(t *testing.T) {
		ctx := &DBContext{Cluster: "invalid"}
		s, err := ctx.prepare(context.Background(), "fail")
		assert.Error(t, err)
		assert.Nil(t, s)
	})
}

func TestDBContext_Query(t *testing.T) {
	rows := new(sql.Rows)
	ctx := &DBContext{
		QueryFn: func(query string, args ...any) (*sql.Rows, error) {
			return rows, nil
		},
	}
	r, err := ctx.Query(context.Background(), "Q", 1)
	assert.NoError(t, err)
	assert.Equal(t, rows, r)
}

func TestDBContext_Query_FallbackAndError(t *testing.T) {
	rows := new(sql.Rows)

	t.Run("FallbackToConnQuery", func(t *testing.T) {
		conn := new(MockedMySqlDb)
		conn.On("Query", "fallback-q", mock.Anything).Return(rows, nil)

		ctx := &DBContext{Conn: conn}
		r, err := ctx.Query(context.Background(), "fallback-q", 1)
		assert.NoError(t, err)
		assert.Equal(t, rows, r)
	})

	t.Run("QueryFnError", func(t *testing.T) {
		ctx := &DBContext{
			QueryFn: func(query string, args ...any) (*sql.Rows, error) {
				return nil, assert.AnError
			},
		}
		r, err := ctx.Query(context.Background(), "q", 1)
		assert.Error(t, err)
		assert.Nil(t, r)
	})
}

func TestDBContext_query(t *testing.T) {
	rows := new(sql.Rows)

	t.Run("UsingTx", func(t *testing.T) {
		tx := new(MockTx)
		tx.On("Query", "q11", mock.Anything).Return(rows, nil)
		ctx := &DBContext{Tx: tx}
		r, err := ctx.query(context.Background(), "q11", 1)
		assert.NoError(t, err)
		assert.Equal(t, rows, r)
	})

	t.Run("UsingConn", func(t *testing.T) {
		conn := new(MockedMySqlDb)
		conn.On("Query", "q22", mock.Anything).Return(rows, nil)
		ctx := &DBContext{Conn: conn}
		r, err := ctx.query(context.Background(), "q22", 1)
		assert.NoError(t, err)
		assert.Equal(t, rows, r)
	})

	t.Run("UsingCluster", func(t *testing.T) {
		SetConnectionConfig("z", &ConnectionConfig{Host: "h", DbName: "db"})
		db := new(MockedMySqlDb)
		db.On("Ping").Return(nil)
		db.On("Query", "q33", mock.Anything).Return(rows, nil)
		c := new(MockedMySqlDbConnector)
		c.On("Open", "mysql", mock.Anything).Return(db, nil)

		original := helperMySqlConnector
		helperMySqlConnector = c
		t.Cleanup(func() {
			helperMySqlConnector = original
		})

		ctx := &DBContext{Cluster: "z"}
		r, err := ctx.query(context.Background(), "q33", 1)
		assert.NoError(t, err)
		assert.Equal(t, rows, r)
	})

	t.Run("InvalidCluster", func(t *testing.T) {
		ctx := &DBContext{Cluster: "bad"}
		r, err := ctx.query(context.Background(), "q44", 1)
		assert.Error(t, err)
		assert.Nil(t, r)
	})
}
