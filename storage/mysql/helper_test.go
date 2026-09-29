package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mysqlErrCloser struct{}

func (mysqlErrCloser) Close() error {
	return errors.New("close failed")
}

func TestCloseSQLCloser_IgnoresCloseError(t *testing.T) {
	closeSQLCloser(mysqlErrCloser{})
}

func TestExecuteReadQuery(t *testing.T) {
	t.Run("NilContextShouldFail", func(t *testing.T) {
		res, err := ExecuteReadQuery(context.Background(), nil, ReadQueryInput{})
		assert.Nil(t, res)
		assert.ErrorContains(t, err, "nil DB context")
	})

	t.Run("QueryFails", func(t *testing.T) {
		ctx := &DBContext{
			QueryContextFn: func(context.Context, string, ...any) (*sql.Rows, error) {
				return nil, fmt.Errorf("query error")
			},
		}

		res, err := ExecuteReadQuery(context.Background(), ctx, ReadQueryInput{Query: "BAD SQL"})
		assert.Nil(t, res)
		assert.ErrorContains(t, err, "query error")
	})
}

func TestExecuteWriteQuery(t *testing.T) {
	query := "INSERT INTO test_table (col1) VALUES (?)"
	params := []interface{}{"value1"}

	t.Run("InvalidDBContext", func(t *testing.T) {
		_, _, err := ExecuteWriteQuery(context.Background(), nil, query, params)
		assert.Error(t, err)
	})

	t.Run("SuccessfulInsert", func(t *testing.T) {
		mockDB := new(MockedMySqlDb)
		mockDB.On("Ping").Return(nil)
		mockConnector := new(MockedMySqlDbConnector)
		mockConnector.On("Open", "mysql", mock.Anything).Return(mockDB, nil)

		mockResult := new(MockedSqlResult)
		mockResult.On("LastInsertId").Return(int64(10), nil)
		mockResult.On("RowsAffected").Return(int64(1), nil)

		mockDB.On("Exec", query, params).Return(mockResult, nil)

		original := helperMySqlConnector
		helperMySqlConnector = mockConnector
		t.Cleanup(func() {
			helperMySqlConnector = original
		})

		SetConnectionConfig("success-cluster", &ConnectionConfig{Host: "localhost", Port: "3306"})

		rows, id, err := ExecuteWriteQuery(context.Background(), &DBContext{Cluster: "success-cluster"}, query, params)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), rows)
		assert.Equal(t, int64(10), id)
	})

	t.Run("DBConnectionFailure", func(t *testing.T) {
		mockDB := new(MockedMySqlDb) // Return a non-nil dummy db to satisfy interface assertion
		mockDB.On("Ping").Return(fmt.Errorf("db connection error"))

		mockConnector := new(MockedMySqlDbConnector)
		mockConnector.On("Open", "mysql", mock.Anything).Return(mockDB, nil) // still returns nil error

		original := helperMySqlConnector
		helperMySqlConnector = mockConnector
		t.Cleanup(func() {
			helperMySqlConnector = original
		})

		SetConnectionConfig("fail-cluster", &ConnectionConfig{Host: "localhost", Port: "3306"})

		rows, id, err := ExecuteWriteQuery(context.Background(), &DBContext{Cluster: "fail-cluster"}, query, params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "db connection error")
		assert.Equal(t, int64(0), rows)
		assert.Equal(t, int64(0), id)

		mockDB.AssertExpectations(t)
		mockConnector.AssertExpectations(t)
	})

	t.Run("ExecFailure", func(t *testing.T) {
		mockDB := new(MockedMySqlDb)
		mockDB.On("Ping").Return(nil)
		mockConnector := new(MockedMySqlDbConnector)
		mockConnector.On("Open", "mysql", mock.Anything).Return(mockDB, nil)

		mockDB.On("Exec", query, params).Return(nil, fmt.Errorf("exec failure"))

		original := helperMySqlConnector
		helperMySqlConnector = mockConnector
		t.Cleanup(func() {
			helperMySqlConnector = original
		})

		SetConnectionConfig("exec-fail", &ConnectionConfig{Host: "localhost", Port: "3306"})

		rows, id, err := ExecuteWriteQuery(context.Background(), &DBContext{Cluster: "exec-fail"}, query, params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exec failure")
		assert.Equal(t, int64(0), rows)
		assert.Equal(t, int64(0), id)
	})

	t.Run("LastInsertIdError", func(t *testing.T) {
		mockDB := new(MockedMySqlDb)
		mockDB.On("Ping").Return(nil)
		mockConnector := new(MockedMySqlDbConnector)
		mockConnector.On("Open", "mysql", mock.Anything).Return(mockDB, nil)

		mockResult := new(MockedSqlResult)
		mockResult.On("LastInsertId").Return(int64(0), fmt.Errorf("unsupported"))
		mockResult.On("RowsAffected").Return(int64(2), nil)

		mockDB.On("Exec", query, params).Return(mockResult, nil)

		original := helperMySqlConnector
		helperMySqlConnector = mockConnector
		t.Cleanup(func() {
			helperMySqlConnector = original
		})

		SetConnectionConfig("lastid-fail", &ConnectionConfig{Host: "localhost", Port: "3306"})

		rows, id, err := ExecuteWriteQuery(context.Background(), &DBContext{Cluster: "lastid-fail"}, query, params)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), rows)
		assert.Equal(t, int64(0), id)
	})

	t.Run("RowsAffectedError", func(t *testing.T) {
		mockDB := new(MockedMySqlDb)
		mockDB.On("Ping").Return(nil)
		mockConnector := new(MockedMySqlDbConnector)
		mockConnector.On("Open", "mysql", mock.Anything).Return(mockDB, nil)

		mockResult := new(MockedSqlResult)
		mockResult.On("LastInsertId").Return(int64(5), nil)
		mockResult.On("RowsAffected").Return(int64(0), fmt.Errorf("rows error"))

		mockDB.On("Exec", query, params).Return(mockResult, nil)

		original := helperMySqlConnector
		helperMySqlConnector = mockConnector
		t.Cleanup(func() {
			helperMySqlConnector = original
		})

		SetConnectionConfig("rowsfail-cluster", &ConnectionConfig{Host: "localhost", Port: "3306"})

		rows, id, err := ExecuteWriteQuery(context.Background(), &DBContext{Cluster: "rowsfail-cluster"}, query, params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rows error")
		assert.Equal(t, int64(0), rows)
		assert.Equal(t, int64(0), id)
	})
}

func TestMultiInsertFromStructsArray(t *testing.T) {
	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name"`
		Email string `db:"email,omitempty"`
	}

	successUsers := []User{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
	}

	t.Run("InvalidDBContext", func(t *testing.T) {
		_, err := MultiInsertFromStructsArray(context.Background(), nil, "test_users", successUsers)
		assert.Error(t, err)
	})

	t.Run("Success", func(t *testing.T) {
		mockResult := new(MockedSqlResult)
		mockResult.On("RowsAffected").Return(int64(2), nil)

		expectedQuery := "INSERT INTO `test_users` (`id`, `name`, `email`) VALUES (?, ?, DEFAULT), (?, ?, DEFAULT)"
		expectedArgs := []interface{}{1, "Alice", 2, "Bob"}

		mockDb := new(MockedMySqlDb)
		mockDb.On("Exec", expectedQuery, expectedArgs).Return(mockResult, nil)
		//mockDb.On("Exec", expectedQuery, mock.Anything).Return(mockResult, nil)
		mockDb.On("Ping").Return(nil)

		mockConnector := new(MockedMySqlDbConnector)
		mockConnector.On("Open", "mysql", mock.Anything).Return(mockDb, nil)

		original := helperMySqlConnector
		helperMySqlConnector = mockConnector
		t.Cleanup(func() {
			helperMySqlConnector = original
		})

		SetConnectionConfig("test-cluster", &ConnectionConfig{Host: "localhost", Port: "3306"})

		rows, err := MultiInsertFromStructsArray(context.Background(), &DBContext{Cluster: "test-cluster"}, "test_users", successUsers)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), rows)

		mockDb.AssertExpectations(t)
		mockConnector.AssertExpectations(t)
	})

	t.Run("EmptyInputData", func(t *testing.T) {
		rows, err := MultiInsertFromStructsArray(context.Background(), &DBContext{Cluster: "test-cluster"}, "test_users", []User{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "input data array is empty")
		assert.Equal(t, int64(0), rows)
	})

	t.Run("DBConnectionFailure", func(t *testing.T) {
		mockConnector := new(MockedMySqlDbConnector)
		mockConnector.On("Open", "mysql", mock.Anything).Return(new(MySqlDb), fmt.Errorf("connection failed"))

		original := helperMySqlConnector
		helperMySqlConnector = mockConnector
		t.Cleanup(func() {
			helperMySqlConnector = original
		})

		SetConnectionConfig("fail-cluster", &ConnectionConfig{Host: "localhost", Port: "3306"})

		rows, err := MultiInsertFromStructsArray(context.Background(), &DBContext{Cluster: "fail-cluster"}, "test_users", successUsers)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection failed")
		assert.Equal(t, int64(0), rows)
	})

	t.Run("QueryGenerationError", func(t *testing.T) {
		invalid := []struct{ Foo map[string]string }{{Foo: map[string]string{"k": "v"}}}

		mockDb := new(MockedMySqlDb)
		mockDb.On("Ping").Return(nil)

		mockDb.On("Exec", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("exec failed"))

		mockConnector := new(MockedMySqlDbConnector)
		mockConnector.On("Open", "mysql", mock.Anything).Return(mockDb, nil)

		original := helperMySqlConnector
		helperMySqlConnector = mockConnector
		t.Cleanup(func() {
			helperMySqlConnector = original
		})

		SetConnectionConfig("badquery-cluster", &ConnectionConfig{Host: "localhost", Port: "3306"})

		rows, err := MultiInsertFromStructsArray(context.Background(), &DBContext{Cluster: "badquery-cluster"}, "test_users", invalid)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no insertable fields")
		assert.Equal(t, int64(0), rows)
	})

	t.Run("ExecFailure", func(t *testing.T) {
		mockDb := new(MockedMySqlDb)
		mockDb.On("Exec", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("exec failed"))
		mockDb.On("Ping").Return(nil)

		mockConnector := new(MockedMySqlDbConnector)
		mockConnector.On("Open", "mysql", mock.Anything).Return(mockDb, nil)

		original := helperMySqlConnector
		helperMySqlConnector = mockConnector
		t.Cleanup(func() {
			helperMySqlConnector = original
		})

		SetConnectionConfig("execfail-cluster", &ConnectionConfig{Host: "localhost", Port: "3306"})

		rows, err := MultiInsertFromStructsArray(context.Background(), &DBContext{Cluster: "execfail-cluster"}, "test_users", successUsers)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error executing insert")
		assert.Equal(t, int64(0), rows)
	})

	t.Run("RowsAffectedError", func(t *testing.T) {
		mockResult := new(MockedSqlResult)
		mockResult.On("RowsAffected").Return(int64(0), fmt.Errorf("rows affected failed"))

		mockDb := new(MockedMySqlDb)
		mockDb.On("Exec", mock.Anything, mock.Anything).Return(mockResult, nil)
		mockDb.On("Ping").Return(nil)

		mockConnector := new(MockedMySqlDbConnector)
		mockConnector.On("Open", "mysql", mock.Anything).Return(mockDb, nil)

		original := helperMySqlConnector
		helperMySqlConnector = mockConnector
		t.Cleanup(func() {
			helperMySqlConnector = original
		})

		SetConnectionConfig("rowfail-cluster", &ConnectionConfig{Host: "localhost", Port: "3306"})

		rows, err := MultiInsertFromStructsArray(context.Background(), &DBContext{Cluster: "rowfail-cluster"}, "test_users", successUsers)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error fetching impacted rows")
		assert.Equal(t, int64(0), rows)
	})
}

type FailingJSON struct{}

func (f FailingJSON) MarshalJSON() ([]byte, error) {
	return nil, fmt.Errorf("simulated marshal error")
}

type UserWithFailingField struct {
	ID   int         `db:"id"`
	Meta FailingJSON `db:"meta,marshaljson"` // this will trigger marshal error
}

func TestGenerateMultiInsertQueriesFromStructArray(t *testing.T) {
	type Profile struct {
		Age int `json:"age"`
	}

	type User struct {
		ID     int     `db:"id"`
		Name   string  `db:"name,omitempty"`
		Email  string  `db:"email,omitempty"`
		Meta   Profile `db:"meta,marshaljson,omitempty"`
		Ignore string  `db:"-"`
		Extra  string  // No tag, should be ignored
	}

	t.Run("NormalInsertWithAllValues", func(t *testing.T) {
		users := []User{
			{ID: 1, Name: "Alice", Email: "alice@example.com", Meta: Profile{30}},
			{ID: 2, Name: "Bob", Email: "bob@example.com", Meta: Profile{28}},
		}

		query, values, err := generateMultiInsertQueriesFromStructArray("users", users)

		assert.NoError(t, err)
		assert.Contains(t, query, "INSERT INTO `users` (`id`, `name`, `email`, `meta`) VALUES")
		assert.Equal(t, 8, len(values)) // 4 values per row * 2
	})

	t.Run("HandlesOmitemptyWithDefaultValue", func(t *testing.T) {
		users := []User{
			{ID: 1}, // omitempty fields should be DEFAULT
		}

		query, values, err := generateMultiInsertQueriesFromStructArray("users", users)
		assert.NoError(t, err)
		assert.Contains(t, query, "DEFAULT")
		assert.Equal(t, 1, len(values)) // Only ID is added
	})

	t.Run("HandlesPointerInput", func(t *testing.T) {
		users := []*User{
			{ID: 3, Name: "Charlie", Meta: Profile{25}},
		}

		query, values, err := generateMultiInsertQueriesFromStructArray("users", users)
		assert.NoError(t, err)
		assert.Contains(t, query, "INSERT INTO `users`")
		assert.Contains(t, query, "?")
		assert.Equal(t, 3, len(values)) // ID, Name, Meta
	})

	t.Run("MarshalJSONFailure", func(t *testing.T) {
		users := []UserWithFailingField{
			{ID: 1, Meta: FailingJSON{}},
		}

		_, _, err := generateMultiInsertQueriesFromStructArray("test_users", users)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "simulated marshal error")
	})
	t.Run("EmptyInputArray", func(t *testing.T) {
		users := []User{}
		query, values, err := generateMultiInsertQueriesFromStructArray("users", users)
		assert.Error(t, err)
		assert.Equal(t, "", query)
		assert.Nil(t, values)
	})

	t.Run("TagIgnoredField)", func(t *testing.T) {
		type Minimal struct {
			Ignored string `db:"-"`
			ID      int    `db:"id"`
		}

		data := []Minimal{{ID: 1}, {ID: 2}}
		query, values, err := generateMultiInsertQueriesFromStructArray("minimal", data)

		assert.NoError(t, err)
		assert.Contains(t, query, "id")
		assert.NotContains(t, query, "Ignored")
		assert.Equal(t, 2, len(values)) // Only id from 2 rows
	})
}

func TestMySQLIdentifierSafety(t *testing.T) {
	maliciousIdentifiers := []string{
		"users; DROP TABLE users",
		"bad`name",
		"name desc",
		"*",
		"users/*x*/",
		"",
		"db.",
		".table",
	}

	for _, identifier := range maliciousIdentifiers {
		t.Run(identifier, func(t *testing.T) {
			quoted, err := safeMySQLIdent(identifier)
			assert.Error(t, err)
			assert.Empty(t, quoted)
		})
	}

	quoted, err := safeMySQLIdent("app.users")
	assert.NoError(t, err)
	assert.Equal(t, "`app`.`users`", quoted)
}

func TestMySQLHelpersRejectUnsafeIdentifiersBeforeExecution(t *testing.T) {
	dbctx := &DBContext{
		ExecContextFn: func(context.Context, string, ...any) (sql.Result, error) {
			t.Fatal("ExecContext should not run for unsafe identifiers")
			return nil, nil
		},
	}

	_, _, err := InsertFromMap(context.Background(), dbctx, "users; DROP TABLE users", map[string]interface{}{"name": "Alice"})
	assert.Error(t, err)

	_, _, err = InsertFromMap(context.Background(), dbctx, "users", map[string]interface{}{"name desc": "Alice"})
	assert.Error(t, err)

	_, err = UpdateFromMap(context.Background(), dbctx, "users", map[string]interface{}{"bad`name": "Alice"}, "id = ?", 1)
	assert.Error(t, err)

	_, err = DeleteByPrimaryKey(context.Background(), dbctx, "users/*x*/", "id", 1)
	assert.Error(t, err)

	_, err = SoftDeleteByPrimaryKey(context.Background(), dbctx, "users", "deleted;DROP", "id", 1)
	assert.Error(t, err)

	type BadTag struct {
		Name string `db:"name desc"`
	}
	_, _, err = InsertFromStruct(context.Background(), dbctx, "users", BadTag{Name: "Alice"})
	assert.Error(t, err)

	_, _, err = generateMultiInsertQueriesFromStructArray("users", []BadTag{{Name: "Alice"}})
	assert.Error(t, err)

	type GoodTag struct {
		Name string `db:"name"`
	}
	_, _, err = generateMultiInsertQueriesFromStructArray("users; DROP TABLE users", []GoodTag{{Name: "Alice"}})
	assert.Error(t, err)

	_, _, err = InsertFromStruct(context.Background(), dbctx, "users; DROP TABLE users", GoodTag{Name: "Alice"})
	assert.Error(t, err)

	_, err = UpdateFromMap(context.Background(), dbctx, "users; DROP TABLE users", map[string]interface{}{"name": "Alice"}, "id = ?", 1)
	assert.Error(t, err)

	_, err = SoftDeleteByPrimaryKey(context.Background(), dbctx, "users; DROP TABLE users", "deleted", "id", 1)
	assert.Error(t, err)
}

func TestMySQLHelpersRejectInvalidStructShapes(t *testing.T) {
	_, _, err := generateMultiInsertQueriesFromStructArray("users", []interface{}{
		struct {
			ID int `db:"id"`
		}{ID: 1},
		"not-a-struct",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected struct input")

	_, _, err = generateMultiInsertQueriesFromStructArray("users", []string{"not-a-struct"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected struct input")

	type GoodTag struct {
		ID int `db:"id"`
	}
	_, _, err = generateMultiInsertQueriesFromStructArray("users", []*GoodTag{nil})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil pointer")

	_, _, err = generateMultiInsertQueriesFromStructArray("users", []interface{}{
		struct {
			ID int `db:"id"`
		}{ID: 1},
		struct {
			Name string `db:"name"`
		}{Name: "Alice"},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected struct type")

	_, _, err = InsertFromStruct(context.Background(), &DBContext{}, "users", "not-a-struct")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected struct input")

	type EmptyByOmit struct {
		Name string `db:"name,omitempty"`
	}
	_, _, err = InsertFromStruct(context.Background(), &DBContext{}, "users", EmptyByOmit{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no insertable fields")
}

func TestMySQLPrimaryKeyHelpersRejectEveryUnsafeIdentifier(t *testing.T) {
	dbctx := &DBContext{
		ExecContextFn: func(context.Context, string, ...any) (sql.Result, error) {
			t.Fatal("ExecContext should not run for unsafe identifiers")
			return nil, nil
		},
	}

	_, err := DeleteByPrimaryKey(context.Background(), dbctx, "users", "id;DROP", 1)
	assert.Error(t, err)

	_, err = SoftDeleteByPrimaryKey(context.Background(), dbctx, "users", "deleted", "id desc", 1)
	assert.Error(t, err)
}

func TestMySQLMapHelpersAreDeterministicAndParameterized(t *testing.T) {
	const injectedValue = "Robert'); DROP TABLE users;--"
	data := map[string]interface{}{
		"z_col": injectedValue,
		"a_col": 101,
	}

	var capturedQuery string
	var capturedArgs []any
	dbctx := &DBContext{
		ExecContextFn: func(_ context.Context, query string, args ...any) (sql.Result, error) {
			capturedQuery = query
			capturedArgs = append([]any(nil), args...)
			result := new(MockedSqlResult)
			result.On("RowsAffected").Return(int64(1), nil)
			result.On("LastInsertId").Return(int64(10), nil)
			return result, nil
		},
	}

	for i := 0; i < 5; i++ {
		capturedQuery = ""
		capturedArgs = nil
		rows, id, err := InsertFromMap(context.Background(), dbctx, "users", data)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), rows)
		assert.Equal(t, int64(10), id)
		assert.Equal(t, "INSERT INTO `users` (`a_col`, `z_col`) VALUES (?, ?)", capturedQuery)
		assert.Equal(t, []any{101, injectedValue}, capturedArgs)
		assert.NotContains(t, capturedQuery, injectedValue)
	}

	capturedQuery = ""
	capturedArgs = nil
	rows, err := UpdateFromMap(context.Background(), dbctx, "users", data, "id = ?", 77)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rows)
	assert.Equal(t, "UPDATE `users` SET `a_col` = ?, `z_col` = ? WHERE id = ?", capturedQuery)
	assert.Equal(t, []any{101, injectedValue, 77}, capturedArgs)
	assert.NotContains(t, capturedQuery, injectedValue)
}

func TestMySQLHelperOperationHook(t *testing.T) {
	original := mysqlHelperOperationHook
	t.Cleanup(func() {
		mysqlHelperOperationHook = original
	})

	events := make([]mysqlHelperOperationEvent, 0, 2)
	mysqlHelperOperationHook = func(event mysqlHelperOperationEvent) {
		events = append(events, event)
	}

	mockResult := new(MockResult)
	execParams := []interface{}{"alice", 7}
	execDB := &DBContext{
		ExecContextFn: func(ctx context.Context, query string, args ...any) (sql.Result, error) {
			assert.NoError(t, ctx.Err())
			assert.Equal(t, "INSERT INTO `users` (`name`, `id`) VALUES (?, ?)", query)
			assert.Equal(t, execParams, args)
			return mockResult, nil
		},
	}

	result, err := execMySQLHelperOperation(
		context.Background(),
		execDB,
		"insert_map",
		"INSERT INTO `users` (`name`, `id`) VALUES (?, ?)",
		execParams...,
	)
	assert.NoError(t, err)
	assert.Equal(t, mockResult, result)
	execParams[0] = "mutated-after-hook"

	queryErr := assert.AnError
	queryDB := &DBContext{
		QueryContextFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
			assert.NoError(t, ctx.Err())
			assert.Equal(t, "SELECT * FROM `users` WHERE `id` = ?", query)
			assert.Equal(t, []interface{}{7}, args)
			return nil, queryErr
		},
	}

	rows, err := queryMySQLHelperOperation(
		context.Background(),
		queryDB,
		"execute_read",
		"SELECT * FROM `users` WHERE `id` = ?",
		7,
	)
	assert.Nil(t, rows)
	assert.ErrorIs(t, err, queryErr)

	assert.Len(t, events, 2)
	assert.Equal(t, "insert_map", events[0].Operation)
	assert.Equal(t, "INSERT INTO `users` (`name`, `id`) VALUES (?, ?)", events[0].Query)
	assert.Equal(t, []interface{}{"alice", 7}, events[0].Args)
	assert.NoError(t, events[0].Err)
	assert.Equal(t, "execute_read", events[1].Operation)
	assert.Equal(t, "SELECT * FROM `users` WHERE `id` = ?", events[1].Query)
	assert.Equal(t, []interface{}{7}, events[1].Args)
	assert.ErrorIs(t, events[1].Err, queryErr)
}

func TestUpdateFromMapRejectsEmptyInputs(t *testing.T) {
	dbctx := &DBContext{
		ExecContextFn: func(context.Context, string, ...any) (sql.Result, error) {
			t.Fatal("ExecContext should not run for invalid update input")
			return nil, nil
		},
	}

	rows, err := UpdateFromMap(context.Background(), dbctx, "users", map[string]interface{}{}, "id = ?", 1)
	assert.Error(t, err)
	assert.Equal(t, int64(0), rows)

	rows, err = UpdateFromMap(context.Background(), dbctx, "users", map[string]interface{}{"name": "Alice"}, "   ", 1)
	assert.Error(t, err)
	assert.Equal(t, int64(0), rows)
}

func TestInsertFromMapNilDBContextAfterValidSQL(t *testing.T) {
	rows, id, err := InsertFromMap(context.Background(), nil, "users", map[string]interface{}{"name": "Alice"})
	assert.Error(t, err)
	assert.Equal(t, int64(0), rows)
	assert.Equal(t, int64(0), id)
}

func TestGetParameterizedInClause(t *testing.T) {
	tests := []struct {
		name         string
		columnName   string
		columnValues []int
		expectedSQL  string
		expectedMap  map[string]interface{}
	}{
		{
			name:         "empty array",
			columnName:   "id",
			columnValues: []int{},
			expectedSQL:  "",
			expectedMap:  map[string]interface{}{},
		},
		{
			name:         "single value",
			columnName:   "id",
			columnValues: []int{1},
			expectedSQL:  ":id1",
			expectedMap:  map[string]interface{}{":id1": 1},
		},
		{
			name:         "multiple values",
			columnName:   "id",
			columnValues: []int{1, 2, 3},
			expectedSQL:  ":id1,:id2,:id3",
			expectedMap:  map[string]interface{}{":id1": 1, ":id2": 2, ":id3": 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultSQL, resultMap := GetParameterizedInClause(tt.columnName, tt.columnValues)

			if resultSQL != tt.expectedSQL {
				t.Errorf("expected SQL: %v, got: %v", tt.expectedSQL, resultSQL)
			}

			if !reflect.DeepEqual(resultMap, tt.expectedMap) {
				t.Errorf("expected map: %v, got: %v", tt.expectedMap, resultMap)
			}
		})
	}
}

func TestConvertQueryAndNamedParams(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		params       map[string]interface{}
		expectedSQL  string
		expectedVals []interface{}
	}{
		{
			name:         "single named parameter",
			query:        "SELECT * FROM users WHERE id = :id",
			params:       map[string]interface{}{":id": 42},
			expectedSQL:  "SELECT * FROM users WHERE id = ?",
			expectedVals: []interface{}{42},
		},
		{
			name:         "multiple named parameters",
			query:        "SELECT * FROM users WHERE id = :id AND name = :name",
			params:       map[string]interface{}{":id": 42, ":name": "Alice"},
			expectedSQL:  "SELECT * FROM users WHERE id = ? AND name = ?",
			expectedVals: []interface{}{42, "Alice"},
		},
		{
			name:         "missing parameter",
			query:        "SELECT * FROM users WHERE id = :id AND name = :name",
			params:       map[string]interface{}{":id": 42},
			expectedSQL:  "SELECT * FROM users WHERE id = ? AND name = ?",
			expectedVals: []interface{}{42, nil}, // name is missing
		},
		{
			name:         "no parameters",
			query:        "SELECT * FROM users",
			params:       map[string]interface{}{},
			expectedSQL:  "SELECT * FROM users",
			expectedVals: []interface{}{},
		},
		{
			name:         "complex query with special characters",
			query:        "SELECT * FROM users WHERE age > :age AND status = :status ORDER BY :orderColumn",
			params:       map[string]interface{}{":age": 30, ":status": "active", ":orderColumn": "name"},
			expectedSQL:  "SELECT * FROM users WHERE age > ? AND status = ? ORDER BY ?",
			expectedVals: []interface{}{30, "active", "name"},
		},
		{
			name:         "duplicate parameters",
			query:        "SELECT * FROM users WHERE id = :id AND name = :name AND id = :id",
			params:       map[string]interface{}{":id": 42, ":name": "Alice"},
			expectedSQL:  "SELECT * FROM users WHERE id = ? AND name = ? AND id = ?",
			expectedVals: []interface{}{42, "Alice", 42},
		},
		{
			name:         "parameter not in params",
			query:        "SELECT * FROM users WHERE id = :id AND age = :age",
			params:       map[string]interface{}{":id": 42},
			expectedSQL:  "SELECT * FROM users WHERE id = ? AND age = ?",
			expectedVals: []interface{}{42, nil}, // age is missing
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultSQL, resultVals := ConvertQueryAndNamedParams(tt.query, tt.params)

			if resultSQL != tt.expectedSQL {
				t.Errorf("expected SQL: %v, got: %v", tt.expectedSQL, resultSQL)
			}

			if len(resultVals) != len(tt.expectedVals) {
				t.Errorf("expected values count: %v, got count: %v", len(tt.expectedVals), len(resultVals))
			} else {

				expectedMap := make(map[interface{}]int)
				resultMap := make(map[interface{}]int)

				for _, val := range tt.expectedVals {
					expectedMap[val]++
				}

				for _, val := range resultVals {
					resultMap[val]++
				}

				if !reflect.DeepEqual(expectedMap, resultMap) {
					t.Errorf("expected values: %v, got: %v", tt.expectedVals, resultVals)
				}
			}
		})
	}
}

func TestHashKey(t *testing.T) {
	t.Run("StableHashingWithSameArgsInDifferentOrder", func(t *testing.T) {
		query := "SELECT * FROM users WHERE id = ?"
		args1 := map[string]interface{}{
			"user_id": 42,
			"name":    "Alice",
		}
		args2 := map[string]interface{}{
			"name":    "Alice",
			"user_id": 42,
		}

		hash1, err := HashKey(query, args1)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		hash2, err := HashKey(query, args2)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if hash1 != hash2 {
			t.Errorf("Expected same hash for same args in different order, got %s vs %s", hash1, hash2)
		}
	})

	t.Run("DifferentArgsGiveDifferentHashes", func(t *testing.T) {
		query := "SELECT * FROM orders"
		args1 := map[string]interface{}{"id": 101}
		args2 := map[string]interface{}{"id": 102}

		hash1, _ := HashKey(query, args1)
		hash2, _ := HashKey(query, args2)

		if hash1 == hash2 {
			t.Error("Expected different hashes for different args")
		}
	})

	t.Run("EmptyArgsShouldReturnValidHash", func(t *testing.T) {
		hash, err := HashKey("SELECT * FROM products", map[string]interface{}{})
		if err != nil || hash == "" {
			t.Errorf("Expected valid hash, got %v, err: %v", hash, err)
		}
	})

	t.Run("HandlesSpecialCharacters", func(t *testing.T) {
		hash, err := HashKey("UPDATE users SET name = ?", map[string]interface{}{"name": "Alice@123!%"})
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if hash == "" {
			t.Error("Expected non-empty hash")
		}
		if _, err := base64.URLEncoding.DecodeString(hash); err != nil {
			t.Errorf("Hash is not valid base64: %v", err)
		}
	})

	t.Run("DeterministicKeyOrdering", func(t *testing.T) {
		args := map[string]interface{}{"z": "end", "a": "start", "m": "middle"}
		keys := make([]string, 0, len(args))
		for k := range args {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		expected := []string{"a", "m", "z"}
		if strings.Join(keys, "") != strings.Join(expected, "") {
			t.Errorf("Expected keys sorted as %v, got %v", expected, keys)
		}
	})
}

func TestUpdateFromMap(t *testing.T) {
	cluster := "test_cluster"
	tableName := "test_table"
	whereClause := "id = ?"
	data := map[string]interface{}{"name": "Charlie"}
	params := []interface{}{101}
	expectedQuery := "UPDATE `test_table` SET `name` = ? WHERE id = ?"
	expectedArgs := []interface{}{"Charlie", 101}

	t.Run("InvalidDBContext", func(t *testing.T) {
		_, err := UpdateFromMap(context.Background(), nil, tableName, data, whereClause, params...)
		assert.Error(t, err)
	})

	t.Run("SuccessfulUpdate", func(t *testing.T) {
		mockResult := new(MockedSqlResult)
		mockResult.On("RowsAffected").Return(int64(1), nil)

		mockDb := new(MockedMySqlDb)
		mockDb.On("Exec", expectedQuery, expectedArgs).Return(mockResult, nil)

		connectionConfigMap = map[string]*ConnectionConfig{cluster: {}}
		instances = map[string]MySqlDbInterface{cluster: mockDb}

		rows, err := UpdateFromMap(context.Background(), &DBContext{Cluster: cluster}, tableName, data, whereClause, params...)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), rows)

		mockDb.AssertExpectations(t)
		mockResult.AssertExpectations(t)
	})

	t.Run("DBConnectionFailure", func(t *testing.T) {
		instances = map[string]MySqlDbInterface{}
		SetConnectionConfig("invalid_cluster", &ConnectionConfig{Host: "localhost", Port: "3306"})

		mockDb := new(MockedMySqlDb)

		connector := new(MockedMySqlDbConnector)
		connector.On("Open", "mysql", mock.Anything).Return(mockDb, fmt.Errorf("forced connection failure"))

		original := helperMySqlConnector
		helperMySqlConnector = connector
		t.Cleanup(func() {
			helperMySqlConnector = original
		})

		rows, err := UpdateFromMap(context.Background(), &DBContext{Cluster: "invalid_cluster"}, tableName, data, whereClause, params...)
		assert.Error(t, err)
		assert.Equal(t, int64(0), rows)
	})

	t.Run("ExecReturnsError", func(t *testing.T) {
		mockDb := new(MockedMySqlDb)
		mockDb.On("Exec", expectedQuery, expectedArgs).Return(nil, errors.New("forced exec failure"))

		connectionConfigMap = map[string]*ConnectionConfig{cluster: {}}
		instances = map[string]MySqlDbInterface{cluster: mockDb}

		rows, err := UpdateFromMap(context.Background(), &DBContext{Cluster: cluster}, tableName, data, whereClause, params...)
		assert.Error(t, err)
		assert.Equal(t, int64(0), rows)
		assert.Contains(t, err.Error(), "forced exec failure")

		mockDb.AssertExpectations(t)
	})

	t.Run("RowsAffectedReturnsError", func(t *testing.T) {
		mockResult := new(MockedSqlResult)
		mockResult.On("RowsAffected").Return(int64(0), errors.New("rows error"))

		mockDb := new(MockedMySqlDb)
		mockDb.On("Exec", expectedQuery, expectedArgs).Return(mockResult, nil)

		instances = map[string]MySqlDbInterface{cluster: mockDb}

		rows, err := UpdateFromMap(context.Background(), &DBContext{Cluster: cluster}, tableName, data, whereClause, params...)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), rows)

		mockDb.AssertExpectations(t)
		mockResult.AssertExpectations(t)
	})
}

func TestInsertFromMap(t *testing.T) {
	cluster := "test_cluster"
	tableName := "test_table"
	query := "INSERT INTO `test_table` (`name`) VALUES (?)"
	data := map[string]interface{}{"name": "Bob"}

	t.Run("InvalidDBContext", func(t *testing.T) {
		_, _, err := InsertFromMap(context.Background(), nil, "users", make(map[string]interface{}))
		assert.Error(t, err)
	})

	t.Run("SuccessfulInsert", func(t *testing.T) {
		mockResult := new(MockedSqlResult)
		mockResult.On("RowsAffected").Return(int64(1), nil)
		mockResult.On("LastInsertId").Return(int64(1001), nil)

		mdb := new(MockedMySqlDb)
		mdb.On("Exec", query, mock.Anything).Return(mockResult, nil)

		connectionConfigMap = map[string]*ConnectionConfig{cluster: {}}
		instances = map[string]MySqlDbInterface{cluster: mdb}

		rowsAffected, insertId, err := InsertFromMap(context.Background(), &DBContext{Cluster: cluster}, tableName, data)

		assert.NoError(t, err)
		assert.Equal(t, int64(1), rowsAffected)
		assert.Equal(t, int64(1001), insertId)

		mdb.AssertExpectations(t)
		mockResult.AssertExpectations(t)
	})

	t.Run("DBConnectionFailure", func(t *testing.T) {
		instances = map[string]MySqlDbInterface{}
		rowsAffected, insertId, err := InsertFromMap(context.Background(), &DBContext{Cluster: "invalid_cluster"}, tableName, data)
		assert.Error(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Equal(t, int64(0), insertId)
	})

	t.Run("ExecError", func(t *testing.T) {
		mdb := new(MockedMySqlDb)
		mdb.On("Exec", query, mock.Anything).Return(nil, errors.New("forced exec error"))

		connectionConfigMap = map[string]*ConnectionConfig{cluster: {}}
		instances = map[string]MySqlDbInterface{cluster: mdb}

		rowsAffected, insertId, err := InsertFromMap(context.Background(), &DBContext{Cluster: cluster}, tableName, data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "forced exec error")
		assert.Equal(t, int64(0), rowsAffected)
		assert.Equal(t, int64(0), insertId)

		mdb.AssertExpectations(t)
	})

	t.Run("RowsAffectedReturnsError", func(t *testing.T) {
		mockResult := new(MockedSqlResult)
		mockResult.On("RowsAffected").Return(int64(0), errors.New("rows error"))
		mockResult.On("LastInsertId").Return(int64(1002), nil)

		mdb := new(MockedMySqlDb)
		mdb.On("Exec", query, mock.Anything).Return(mockResult, nil)

		instances = map[string]MySqlDbInterface{cluster: mdb}

		rowsAffected, insertId, err := InsertFromMap(context.Background(), &DBContext{Cluster: cluster}, tableName, data)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), rowsAffected)
		assert.Equal(t, int64(1002), insertId)

		mdb.AssertExpectations(t)
		mockResult.AssertExpectations(t)
	})

	t.Run("LastInsertIdReturnsError", func(t *testing.T) {
		mockResult := new(MockedSqlResult)
		mockResult.On("RowsAffected").Return(int64(2), nil)
		mockResult.On("LastInsertId").Return(int64(0), errors.New("id error"))

		mdb := new(MockedMySqlDb)
		mdb.On("Exec", query, mock.Anything).Return(mockResult, nil)

		instances = map[string]MySqlDbInterface{cluster: mdb}

		rowsAffected, insertId, err := InsertFromMap(context.Background(), &DBContext{Cluster: cluster}, tableName, data)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), rowsAffected)
		assert.Equal(t, int64(0), insertId)

		mdb.AssertExpectations(t)
		mockResult.AssertExpectations(t)
	})
}

func TestInsertFromStruct(t *testing.T) {
	type Inner struct {
		Field string `json:"field"`
	}

	type User struct {
		ID    int    `db:"id"`
		Name  string `db:"name,omitempty"`
		Meta  Inner  `db:"meta,marshaljson"`
		Empty string `db:"skip,omitempty"`
		NoTag string // Should be skipped
	}

	t.Run("InvalidDBContext", func(t *testing.T) {
		_, _, err := InsertFromStruct(context.Background(), nil, "users", new(User))
		assert.Error(t, err)
	})

	t.Run("SuccessfulInsert", func(t *testing.T) {
		user := User{ID: 1, Name: "Alice", Meta: Inner{"test"}}

		mockResult := new(MockedSqlResult)
		mockResult.On("RowsAffected").Return(int64(1), nil)
		mockResult.On("LastInsertId").Return(int64(1001), nil)

		mockDb := new(MockedMySqlDb)
		expectedQuery := "INSERT INTO `users` (`id`, `name`, `meta`) VALUES (?, ?, ?)"
		mockDb.On("Exec", expectedQuery, mock.Anything).Return(mockResult, nil)

		connectionConfigMap = map[string]*ConnectionConfig{"test-cluster": {}}
		instances = map[string]MySqlDbInterface{"test-cluster": mockDb}

		rows, id, err := InsertFromStruct(context.Background(), &DBContext{Cluster: "test-cluster"}, "users", &user)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), rows)
		assert.Equal(t, int64(1001), id)

		mockDb.AssertExpectations(t)
		mockResult.AssertExpectations(t)
	})

	t.Run("MarshalJSONFails", func(t *testing.T) {
		type Invalid struct {
			Bad func() `db:"bad,marshaljson"` // cannot marshal functions
		}
		input := Invalid{}

		connectionConfigMap = map[string]*ConnectionConfig{"test-cluster": {}}
		instances = map[string]MySqlDbInterface{"test-cluster": new(MockedMySqlDb)}

		_, _, err := InsertFromStruct(context.Background(), &DBContext{Cluster: "test-cluster"}, "badtable", input)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "json: unsupported type")
	})

	t.Run("ExecFails", func(t *testing.T) {
		user := User{ID: 1, Meta: Inner{"x"}}
		mockDb := new(MockedMySqlDb)

		mockDb.On("Exec", mock.Anything, mock.Anything).Return(nil, errors.New("exec error"))
		connectionConfigMap = map[string]*ConnectionConfig{"test-cluster": {}}
		instances = map[string]MySqlDbInterface{"test-cluster": mockDb}

		_, _, err := InsertFromStruct(context.Background(), &DBContext{Cluster: "test-cluster"}, "users", user)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "exec error")
	})

	t.Run("RowsAffectedFails", func(t *testing.T) {
		user := User{ID: 1, Meta: Inner{"x"}}
		mockResult := new(MockedSqlResult)
		mockResult.On("RowsAffected").Return(int64(0), errors.New("rows error"))
		mockResult.On("LastInsertId").Return(int64(1001), nil)

		mockDb := new(MockedMySqlDb)
		mockDb.On("Exec", mock.Anything, mock.Anything).Return(mockResult, nil)

		instances = map[string]MySqlDbInterface{"test-cluster": mockDb}
		rows, id, err := InsertFromStruct(context.Background(), &DBContext{Cluster: "test-cluster"}, "users", user)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), rows)
		assert.Equal(t, int64(1001), id)
	})

	t.Run("LastInsertIdFails", func(t *testing.T) {
		user := User{ID: 1, Meta: Inner{"x"}}
		mockResult := new(MockedSqlResult)
		mockResult.On("RowsAffected").Return(int64(1), nil)
		mockResult.On("LastInsertId").Return(int64(0), errors.New("id error"))

		mockDb := new(MockedMySqlDb)
		mockDb.On("Exec", mock.Anything, mock.Anything).Return(mockResult, nil)

		instances = map[string]MySqlDbInterface{"test-cluster": mockDb}
		rows, id, err := InsertFromStruct(context.Background(), &DBContext{Cluster: "test-cluster"}, "users", user)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), rows)
		assert.Equal(t, int64(0), id)
	})

	t.Run("DBConnectionFailure", func(t *testing.T) {
		instances = map[string]MySqlDbInterface{}
		connectionConfigMap = map[string]*ConnectionConfig{}

		_, _, err := InsertFromStruct(context.Background(), &DBContext{Cluster: "invalid-cluster"}, "users", User{})
		assert.Error(t, err)
	})
}

func TestDeleteByPrimaryKey(t *testing.T) {
	query := "DELETE FROM `test_table` WHERE `id` = ?"

	t.Run("InvalidDBContext", func(t *testing.T) {
		_, err := DeleteByPrimaryKey(context.Background(), nil, "test_table", "id", 101)
		assert.Error(t, err)
	})

	t.Run("SuccessfulDeletion", func(t *testing.T) {
		mockDb := new(MockedMySqlDb)
		mockResult := new(MockedSqlResult)

		mockDb.On("Exec", query, mock.MatchedBy(func(args []interface{}) bool {
			return len(args) == 1 && args[0] == 101
		})).Return(mockResult, nil)

		mockResult.On("RowsAffected").Return(int64(1), nil)

		instances = map[string]MySqlDbInterface{
			"test-cluster": mockDb,
		}

		rows, err := DeleteByPrimaryKey(context.Background(), &DBContext{Cluster: "test-cluster"}, "test_table", "id", 101)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), rows)

		mockDb.AssertExpectations(t)
		mockResult.AssertExpectations(t)
	})

	t.Run("DBConnectionFailure", func(t *testing.T) {
		instances = nil // simulate no connection in pool

		rows, err := DeleteByPrimaryKey(context.Background(), &DBContext{Cluster: "unknown-cluster"}, "test_table", "id", 101)
		assert.Error(t, err)
		assert.Equal(t, int64(0), rows)
	})

	t.Run("ExecError", func(t *testing.T) {
		mockDb := new(MockedMySqlDb)
		mockDb.On("Exec", query, mock.MatchedBy(func(args []interface{}) bool {
			return len(args) == 1 && args[0] == 101
		})).Return(nil, errors.New("forced exec error"))

		instances = map[string]MySqlDbInterface{
			"test-cluster": mockDb,
		}

		rows, err := DeleteByPrimaryKey(context.Background(), &DBContext{Cluster: "test-cluster"}, "test_table", "id", 101)
		assert.Error(t, err)
		assert.Equal(t, int64(0), rows)

		mockDb.AssertExpectations(t)
	})

	t.Run("RowsAffectedReturnsError", func(t *testing.T) {
		mockDb := new(MockedMySqlDb)
		mockResult := new(MockedSqlResult)

		mockDb.On("Exec", query, mock.MatchedBy(func(args []interface{}) bool {
			return len(args) == 1 && args[0] == 101
		})).Return(mockResult, nil)

		mockResult.On("RowsAffected").Return(int64(0), fmt.Errorf("fetch error"))

		instances = map[string]MySqlDbInterface{
			"test-cluster": mockDb,
		}

		rows, err := DeleteByPrimaryKey(context.Background(), &DBContext{Cluster: "test-cluster"}, "test_table", "id", 101)
		assert.NoError(t, err)
		assert.Equal(t, int64(0), rows)

		mockDb.AssertExpectations(t)
		mockResult.AssertExpectations(t)
	})
}

func TestSoftDeleteByPrimaryKey(t *testing.T) {
	query := "UPDATE `test_table` SET `is_deleted` = 1 WHERE `id` = ?"

	t.Run("InvalidDBContext", func(t *testing.T) {
		_, err := SoftDeleteByPrimaryKey(context.Background(), nil, "test_table", "is_deleted", "id", 101)
		assert.Error(t, err)
	})

	t.Run("SuccessfulSoftDelete", func(t *testing.T) {
		mockDb := new(MockedMySqlDb)
		mockResult := new(MockedSqlResult)

		mockDb.On("Exec", query, mock.MatchedBy(func(args []interface{}) bool {
			return reflect.DeepEqual(args, []interface{}{101})
		})).Return(mockResult, nil)
		mockResult.On("RowsAffected").Return(int64(1), nil)

		instances = map[string]MySqlDbInterface{
			"test-cluster": mockDb,
		}

		rows, err := SoftDeleteByPrimaryKey(context.Background(), &DBContext{Cluster: "test-cluster"}, "test_table", "is_deleted", "id", 101)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), rows)
		mockDb.AssertExpectations(t)
		mockResult.AssertExpectations(t)
	})

	t.Run("DBConnectionFailure", func(t *testing.T) {
		instances = map[string]MySqlDbInterface{}
		rows, err := SoftDeleteByPrimaryKey(context.Background(), &DBContext{Cluster: "invalid-cluster"}, "test_table", "is_deleted", "id", 101)
		assert.Error(t, err)
		assert.Equal(t, int64(0), rows)
	})

	t.Run("ExecError", func(t *testing.T) {
		mockDb := new(MockedMySqlDb)
		mockDb.On("Exec", query, mock.MatchedBy(func(args []interface{}) bool {
			return reflect.DeepEqual(args, []interface{}{101})
		})).Return(nil, errors.New("forced exec error"))

		instances = map[string]MySqlDbInterface{
			"test-cluster": mockDb,
		}

		rows, err := SoftDeleteByPrimaryKey(context.Background(), &DBContext{Cluster: "test-cluster"}, "test_table", "is_deleted", "id", 101)
		assert.Error(t, err)
		assert.Equal(t, int64(0), rows)
		mockDb.AssertExpectations(t)
	})

	t.Run("RowsAffectedReturnsError", func(t *testing.T) {
		mockDb := new(MockedMySqlDb)
		mockResult := new(MockedSqlResult)

		mockDb.On("Exec", query, mock.MatchedBy(func(args []interface{}) bool {
			return reflect.DeepEqual(args, []interface{}{101})
		})).Return(mockResult, nil)
		mockResult.On("RowsAffected").Return(int64(0), errors.New("rows error"))

		instances = map[string]MySqlDbInterface{
			"test-cluster": mockDb,
		}

		rows, err := SoftDeleteByPrimaryKey(context.Background(), &DBContext{Cluster: "test-cluster"}, "test_table", "is_deleted", "id", 101)
		assert.NoError(t, err) // still returns no error
		assert.Equal(t, int64(0), rows)
		mockDb.AssertExpectations(t)
		mockResult.AssertExpectations(t)
	})
}

func TestMultiInsertFromStructsArray_RowsAffectedError(t *testing.T) {
	type User struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}

	mockResult := new(MockedSqlResult)
	mockResult.On("RowsAffected").Return(int64(0), errors.New("rows affected error"))

	mockDB := new(MockedMySqlDb)
	mockDB.On("Ping").Return(nil)
	mockDB.On("Exec", mock.Anything, mock.Anything).Return(mockResult, nil)

	mockConnector := new(MockedMySqlDbConnector)
	mockConnector.On("Open", "mysql", mock.Anything).Return(mockDB, nil)

	original := helperMySqlConnector
	helperMySqlConnector = mockConnector
	t.Cleanup(func() {
		helperMySqlConnector = original
	})

	SetConnectionConfig("rows-affected-cluster", &ConnectionConfig{Host: "localhost", Port: "3306"})

	rows, err := MultiInsertFromStructsArray(context.Background(), &DBContext{Cluster: "rows-affected-cluster"}, "users", []User{
		{ID: 1, Name: "A"},
		{ID: 2, Name: "B"},
	})
	assert.Error(t, err)
	assert.Equal(t, int64(0), rows)
}

func TestMultiInsertFromStructsArray_GenerationError(t *testing.T) {
	type BadInner struct {
		C chan int `json:"c"`
	}
	type Bad struct {
		Payload BadInner `db:"payload,marshaljson"`
	}

	rows, err := MultiInsertFromStructsArray(context.Background(), &DBContext{Cluster: "rows-affected-cluster"}, "bad", []Bad{
		{Payload: BadInner{C: make(chan int)}},
	})
	assert.Error(t, err)
	assert.Equal(t, int64(0), rows)
}

type failingHashWriter struct{}

func (f *failingHashWriter) Write(p []byte) (int, error) {
	return 0, errors.New("hash write error")
}

func (f *failingHashWriter) Sum(b []byte) []byte { return b }

func TestHashKey_WriteError(t *testing.T) {
	orig := newHashWriter
	newHashWriter = func() hashWriter { return &failingHashWriter{} }
	defer func() { newHashWriter = orig }()

	key, err := HashKey("select 1", map[string]interface{}{"a": 1})
	assert.Error(t, err)
	assert.Equal(t, "", key)
}

const mysqlTestDriverName = "photon_mysql_test_driver"

var registerMySQLTestDriverOnce sync.Once

func registerMySQLTestDriver() {
	registerMySQLTestDriverOnce.Do(func() {
		sql.Register(mysqlTestDriverName, &mysqlReadTestDriver{})
	})
}

type mysqlReadTestDriver struct{}

func (d *mysqlReadTestDriver) Open(name string) (driver.Conn, error) {
	return &mysqlReadTestConn{}, nil
}

type mysqlReadTestConn struct{}

func (c *mysqlReadTestConn) Prepare(query string) (driver.Stmt, error) {
	if strings.Contains(query, "prepare_error") {
		return nil, errors.New("prepare error")
	}
	return &mysqlReadTestStmt{query: query}, nil
}

func (c *mysqlReadTestConn) Close() error { return nil }
func (c *mysqlReadTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c *mysqlReadTestConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return buildMySQLReadRows(query)
}

type mysqlReadTestStmt struct {
	query string
}

func (s *mysqlReadTestStmt) Close() error  { return nil }
func (s *mysqlReadTestStmt) NumInput() int { return -1 }
func (s *mysqlReadTestStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("not implemented")
}
func (s *mysqlReadTestStmt) Query(args []driver.Value) (driver.Rows, error) {
	return buildMySQLReadRows(s.query)
}

type mysqlReadRows struct {
	columns  []string
	values   [][]driver.Value
	idx      int
	nextErr  error
	injected bool
}

func (r *mysqlReadRows) Columns() []string {
	return r.columns
}

func (r *mysqlReadRows) Close() error { return nil }

func (r *mysqlReadRows) Next(dest []driver.Value) error {
	if r.idx < len(r.values) {
		copy(dest, r.values[r.idx])
		r.idx++
		return nil
	}
	if r.nextErr != nil && !r.injected {
		r.injected = true
		return r.nextErr
	}
	return io.EOF
}

func buildMySQLReadRows(query string) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "query_error"):
		return nil, errors.New("query error")
	case strings.Contains(query, "scan_error"):
		return &mysqlReadRows{
			columns: []string{"age"},
			values:  [][]driver.Value{{int64(44)}},
		}, nil
	case strings.Contains(query, "rows_error"):
		return &mysqlReadRows{
			columns: []string{"name"},
			values:  [][]driver.Value{{"alice"}},
			nextErr: errors.New("rows error"),
		}, nil
	default:
		return &mysqlReadRows{
			columns: []string{"name", "age"},
			values:  [][]driver.Value{{"alice", "44"}},
		}, nil
	}
}

func testMySQLReadContext(t *testing.T) *DBContext {
	t.Helper()
	registerMySQLTestDriver()

	db, err := sql.Open(mysqlTestDriverName, "unused")
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return &DBContext{
		Conn: &MySqlDb{DB: db},
	}
}

func TestExecuteReadQuery_SuccessAndCapitalise(t *testing.T) {
	ctx := testMySQLReadContext(t)

	out, err := ExecuteReadQuery(context.Background(), ctx, ReadQueryInput{
		Query:             "select ok",
		CapitaliseColumns: true,
	})
	assert.NoError(t, err)
	if assert.Len(t, out, 1) {
		_, hasLower := out[0]["name"]
		assert.False(t, hasLower)
		_, hasUpper := out[0]["Name"]
		assert.True(t, hasUpper)
	}
}

func TestExecuteReadQuery_QueryAndScanAndRowsErrors(t *testing.T) {
	ctx := testMySQLReadContext(t)

	_, err := ExecuteReadQuery(context.Background(), ctx, ReadQueryInput{Query: "query_error"})
	assert.Error(t, err)

	origScan := scanReadRow
	scanReadRow = func(rows *sql.Rows, dest ...interface{}) error {
		return errors.New("scan error")
	}
	_, err = ExecuteReadQuery(context.Background(), ctx, ReadQueryInput{Query: "scan_error"})
	scanReadRow = origScan
	assert.Error(t, err)

	_, err = ExecuteReadQuery(context.Background(), ctx, ReadQueryInput{Query: "rows_error"})
	assert.Error(t, err)
}

func TestExecuteReadQuery_ColumnsError(t *testing.T) {
	ctx := testMySQLReadContext(t)

	origCols := getReadColumns
	getReadColumns = func(rows *sql.Rows) ([]string, error) {
		return nil, errors.New("columns error")
	}
	defer func() { getReadColumns = origCols }()

	_, err := ExecuteReadQuery(context.Background(), ctx, ReadQueryInput{Query: "select ok"})
	assert.Error(t, err)
}

func TestExecuteReadQuery_Errors(t *testing.T) {
	_, err := ExecuteReadQuery(context.Background(), nil, ReadQueryInput{Query: "SELECT 1"})
	assert.Error(t, err)

	ctx := &DBContext{
		PrepareFn: func(query string) (*sql.Stmt, error) {
			return nil, errors.New("prepare fail")
		},
	}
	_, err = ExecuteReadQuery(context.Background(), ctx, ReadQueryInput{Query: "SELECT 1"})
	assert.Error(t, err)

	ctx = &DBContext{
		PrepareFn: func(query string) (*sql.Stmt, error) {
			return nil, nil
		},
		QueryFn: func(query string, args ...any) (*sql.Rows, error) {
			return nil, errors.New("query fail")
		},
	}
	_, err = ExecuteReadQuery(context.Background(), ctx, ReadQueryInput{Query: "SELECT 1"})
	assert.Error(t, err)
}
