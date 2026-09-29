package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/cshekharsharma/photon/utils/types"
	"github.com/pkg/errors"
)

var helperMySqlConnector MySqlDbConnectorInterface = &MySqlDbConnector{}
var scanReadRow = func(rows *sql.Rows, dest ...interface{}) error { return rows.Scan(dest...) }
var getReadColumns = func(rows *sql.Rows) ([]string, error) { return rows.Columns() }
var newHashWriter = func() hashWriter { return sha256.New() }
var mysqlIdentifierRegexp = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var mysqlHelperOperationHook func(mysqlHelperOperationEvent)

type mysqlHelperOperationEvent struct {
	Operation string
	Query     string
	Args      []interface{}
	Err       error
}

type hashWriter interface {
	Write(p []byte) (n int, err error)
	Sum(b []byte) []byte
}

type sqlCloser interface {
	Close() error
}

func closeSQLCloser(closer sqlCloser) {
	if err := closer.Close(); err != nil {
		return
	}
}

func safeMySQLIdent(ident string) (string, error) {
	ident = strings.TrimSpace(ident)
	if ident == "" {
		return "", fmt.Errorf("empty identifier")
	}

	parts := strings.Split(ident, ".")
	for i, part := range parts {
		if !mysqlIdentifierRegexp.MatchString(part) {
			return "", fmt.Errorf("invalid identifier %q", ident)
		}
		parts[i] = "`" + part + "`"
	}

	return strings.Join(parts, "."), nil
}

func quoteMySQLIdentifiers(fields []string) ([]string, error) {
	quoted := make([]string, 0, len(fields))
	for _, field := range fields {
		identifier, err := safeMySQLIdent(field)
		if err != nil {
			return nil, err
		}
		quoted = append(quoted, identifier)
	}
	return quoted, nil
}

func notifyMySQLHelperOperation(operation string, query string, args []interface{}, err error) {
	if mysqlHelperOperationHook == nil {
		return
	}
	mysqlHelperOperationHook(mysqlHelperOperationEvent{
		Operation: operation,
		Query:     query,
		Args:      append([]interface{}(nil), args...),
		Err:       err,
	})
}

func queryMySQLHelperOperation(
	ctx context.Context,
	dbctx *DBContext,
	operation string,
	query string,
	args ...interface{},
) (*sql.Rows, error) {
	rows, err := dbctx.Query(ctx, query, args...)
	notifyMySQLHelperOperation(operation, query, args, err)
	return rows, err
}

func execMySQLHelperOperation(
	ctx context.Context,
	dbctx *DBContext,
	operation string,
	query string,
	args ...interface{},
) (sql.Result, error) {
	result, err := dbctx.Exec(ctx, query, args...)
	notifyMySQLHelperOperation(operation, query, args, err)
	return result, err
}

// ExecuteReadQuery runs a read-only SELECT query on a given cluster,
// returning results as a slice of maps where each map represents a row.
// If CapitaliseColumns is set to true in the input, column names in the result
// will be capitalized.
//
// Parameters:
//   - cluster: name of the database cluster.
//   - queryInput: struct containing the query string, parameters, and capitalisation flag.
//
// Returns:
//   - []map[string]interface{}: list of result rows.
//   - error: any error encountered while querying.
func ExecuteReadQuery(ctx context.Context, dbctx *DBContext, queryInput ReadQueryInput) ([]map[string]interface{}, error) {
	if dbctx == nil {
		return nil, fmt.Errorf("nil DB context provided, cannot execute the query")
	}

	rows, err := queryMySQLHelperOperation(ctx, dbctx, "execute_read", queryInput.Query, queryInput.Params...)
	if err != nil {
		return nil, err
	}

	if rows != nil {
		defer func() {
			_ = rows.Close()
		}()
	}

	columns, err := getReadColumns(rows)
	if err != nil {
		return nil, err
	}

	var output []map[string]interface{}

	for rows.Next() {
		values := make([]sql.NullString, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := scanReadRow(rows, scanArgs...); err != nil {
			return nil, err
		}

		rowMap := make(map[string]interface{})
		for i, colName := range columns {
			if queryInput.CapitaliseColumns {
				colName = types.UCFirst(colName)
			}
			rowMap[colName] = values[i]
		}

		output = append(output, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return output, nil
}

// ExecuteWriteQuery executes a SQL write query (INSERT/UPDATE/DELETE) using the provided DB context.
// It supports transactional or direct DB execution based on the DBContext.
//
// Parameters:
//   - dbctx: context containing transaction, connection, or cluster.
//   - query: SQL query string.
//   - params: query parameters.
//
// Returns:
//   - rows affected
//   - last insert ID (0 if not applicable)
//   - error if any.
func ExecuteWriteQuery(ctx context.Context, dbctx *DBContext, query string, params []interface{}) (int64, int64, error) {
	if dbctx == nil {
		return 0, 0, fmt.Errorf("nil DB context provided, cannot execute the query")
	}

	result, err := execMySQLHelperOperation(ctx, dbctx, "execute_write", query, params...)

	if err != nil {
		return 0, 0, fmt.Errorf("error executing query: %w", err)
	}

	lastInsertId, err := result.LastInsertId()
	if err != nil {
		lastInsertId = 0 // likely a non-INSERT query; defaulting to 0
	}

	rowsImpacted, err := result.RowsAffected()
	if err != nil {
		return 0, 0, fmt.Errorf("error fetching impacted rows: %w", err)
	}

	return rowsImpacted, lastInsertId, nil
}

// MultiInsertFromStructsArray performs a bulk insert operation using a slice of structs.
// Fields are inferred from struct tags, and default values are handled using `omitempty`.
// Supports JSON marshaling via tag option "marshaljson".
//
// Parameters:
//   - dbctx: database context.
//   - tableName: target table.
//   - data: slice of structs to insert.
//
// Returns:
//   - rows affected
//   - error if any.
func MultiInsertFromStructsArray[T any](ctx context.Context, dbctx *DBContext, tableName string, data []T) (int64, error) {
	if len(data) == 0 {
		return 0, fmt.Errorf("input data array is empty")
	}

	query, allValues, err := generateMultiInsertQueriesFromStructArray(tableName, data)
	if err != nil {
		return 0, errors.Wrap(err, "error in generating query from input data")
	}

	if dbctx == nil {
		return 0, fmt.Errorf("nil DB context provided, cannot execute the query")
	}

	result, err := execMySQLHelperOperation(ctx, dbctx, "multi_insert_structs", query, allValues...)

	if err != nil {
		return 0, fmt.Errorf("error executing insert: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("error fetching impacted rows: %w", err)
	}

	return rowsAffected, nil
}

// generateMultiInsertQueriesFromStructArray constructs an INSERT SQL query string
// and associated values slice from a slice of structs. Fields are parsed from struct tags.
//
// Parameters:
//   - tableName: target table name.
//   - data: slice of structs.
//
// Returns:
//   - SQL query string
//   - slice of values
//   - error if any.
func generateMultiInsertQueriesFromStructArray[T any](tableName string, data []T) (string, []interface{}, error) {
	if len(data) == 0 {
		return "", nil, fmt.Errorf("no data provided")
	}
	quotedTableName, err := safeMySQLIdent(tableName)
	if err != nil {
		return "", nil, err
	}

	dataLen := len(data)
	var queryBuilder strings.Builder

	firstRowValue, err := mysqlStructValue(data[0])
	if err != nil {
		return "", nil, err
	}
	t := firstRowValue.Type()
	fields, quotedFields, err := mysqlInsertFields(t)
	if err != nil {
		return "", nil, err
	}

	allValues := make([]interface{}, 0, dataLen*len(fields))
	valueStrings := make([]string, 0, dataLen)

	for _, row := range data {
		sValue, err := mysqlStructValue(row)
		if err != nil {
			return "", nil, err
		}
		if sValue.Type() != t {
			return "", nil, fmt.Errorf("expected struct type %s, got %s", t, sValue.Type())
		}

		rowPlaceholders, rowValues, err := mysqlInsertRowValues(sValue, t, fields)
		if err != nil {
			return "", nil, err
		}
		allValues = append(allValues, rowValues...)
		valueStrings = append(valueStrings, "("+strings.Join(rowPlaceholders, ", ")+")")
	}

	queryBuilder.WriteString("INSERT INTO ")
	queryBuilder.WriteString(quotedTableName)
	queryBuilder.WriteString(" (")
	queryBuilder.WriteString(strings.Join(quotedFields, ", "))
	queryBuilder.WriteString(") VALUES ")
	queryBuilder.WriteString(strings.Join(valueStrings, ", "))

	return queryBuilder.String(), allValues, nil
}

func mysqlStructValue(row interface{}) (reflect.Value, error) {
	sValue := reflect.ValueOf(row)
	if sValue.Kind() == reflect.Pointer {
		if sValue.IsNil() {
			return reflect.Value{}, fmt.Errorf("expected struct input, got nil pointer")
		}
		sValue = sValue.Elem()
	}
	if sValue.Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("expected struct input, got %s", sValue.Kind())
	}
	return sValue, nil
}

func mysqlInsertFields(t reflect.Type) ([]string, []string, error) {
	fields := make([]string, 0, mysqlTaggedFieldCount(t))
	for i := 0; i < t.NumField(); i++ {
		fieldName := mysqlDBTagName(t.Field(i))
		if fieldName == "" {
			continue
		}
		fields = append(fields, fieldName)
	}

	quotedFields, err := quoteMySQLIdentifiers(fields)
	if err != nil {
		return nil, nil, err
	}
	if len(quotedFields) == 0 {
		return nil, nil, fmt.Errorf("no insertable fields found (check db tags)")
	}
	return fields, quotedFields, nil
}

func mysqlTaggedFieldCount(t reflect.Type) int {
	count := 0
	for i := 0; i < t.NumField(); i++ {
		if mysqlDBTagName(t.Field(i)) != "" {
			count++
		}
	}
	return count
}

func mysqlDBTagName(field reflect.StructField) string {
	dbTag := field.Tag.Get("db")
	if dbTag == "" {
		return ""
	}
	fieldName := strings.Split(dbTag, ",")[0]
	if fieldName == "-" {
		return ""
	}
	return fieldName
}

func mysqlInsertRowValues(
	sValue reflect.Value,
	t reflect.Type,
	fields []string,
) ([]string, []interface{}, error) {
	rowPlaceholders := make([]string, len(fields))
	fieldValues := make(map[string]interface{})
	fieldIsDefault := make(map[string]bool)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldName := mysqlDBTagName(field)
		if fieldName == "" {
			continue
		}

		value, useDefault, err := mysqlInsertFieldValue(sValue.Field(i), field)
		if err != nil {
			return nil, nil, err
		}
		fieldValues[fieldName] = value
		fieldIsDefault[fieldName] = useDefault
	}

	rowValues := make([]interface{}, 0, len(fields))
	for i, fieldName := range fields {
		if fieldIsDefault[fieldName] {
			rowPlaceholders[i] = "DEFAULT"
			continue
		}
		rowPlaceholders[i] = "?"
		rowValues = append(rowValues, fieldValues[fieldName])
	}
	return rowPlaceholders, rowValues, nil
}

func mysqlInsertFieldValue(value reflect.Value, field reflect.StructField) (interface{}, bool, error) {
	tagParts := strings.Split(field.Tag.Get("db"), ",")
	fieldValue := value.Interface()
	useDefault := slices.Contains(tagParts, "omitempty") && types.IsEmpty(fieldValue)
	if useDefault || !slices.Contains(tagParts, "marshaljson") || value.Kind() != reflect.Struct {
		return fieldValue, useDefault, nil
	}

	jsonValue, err := json.Marshal(fieldValue)
	if err != nil {
		return nil, false, err
	}
	return string(jsonValue), false, nil
}

// InsertFromStruct inserts a single record into the given table using field tags from a struct.
// Supports "omitempty" and "marshaljson" in struct tags.
//
// Parameters:
//   - dbctx: DB context.
//   - tableName: target table.
//   - data: struct with db-tagged fields.
//
// Returns:
//   - rows affected
//   - last insert ID
//   - error if any.
func InsertFromStruct(ctx context.Context, dbctx *DBContext, tableName string, data interface{}) (int64, int64, error) {
	quotedTableName, err := safeMySQLIdent(tableName)
	if err != nil {
		return 0, 0, err
	}

	sValue := reflect.ValueOf(data)
	if sValue.Kind() == reflect.Pointer {
		sValue = sValue.Elem()
	}
	if sValue.Kind() != reflect.Struct {
		return 0, 0, fmt.Errorf("expected struct input, got %s", sValue.Kind())
	}

	t := sValue.Type()
	fields := make([]string, 0)
	placeholders := make([]string, 0)
	values := make([]interface{}, 0)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		if dbTag := field.Tag.Get("db"); dbTag != "" {
			value := sValue.Field(i).Interface()
			tagParts := strings.Split(dbTag, ",")
			omitempty := slices.Contains(tagParts, "omitempty")

			if !omitempty || !types.IsEmpty(value) {
				if sValue.Kind() == reflect.Struct {
					if slices.Contains(tagParts, "marshaljson") {
						jsonValue, err := json.Marshal(value)

						if err != nil {
							return 0, 0, err
						}
						value = string(jsonValue)
					}
				}

				quotedField, err := safeMySQLIdent(tagParts[0])
				if err != nil {
					return 0, 0, err
				}
				fields = append(fields, quotedField)
				placeholders = append(placeholders, "?")
				values = append(values, value)
			}
		}
	}
	if len(fields) == 0 {
		return 0, 0, fmt.Errorf("no insertable fields found (check db tags / omitempty)")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quotedTableName, strings.Join(fields, ", "), strings.Join(placeholders, ", "))

	if dbctx == nil {
		return 0, 0, fmt.Errorf("nil DB context provided, cannot execute the query")
	}

	result, execErr := execMySQLHelperOperation(ctx, dbctx, "insert_struct", query, values...)
	if execErr != nil {
		return 0, 0, execErr
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		rowsAffected = 0
	}

	lastInsertId, err := result.LastInsertId()
	if err != nil {
		lastInsertId = 0
	}

	return rowsAffected, lastInsertId, nil
}

// InsertFromMap inserts a single record into the given table using a key-value map
// of column names and their corresponding values.
//
// Parameters:
//   - dbctx: DB context.
//   - tableName: target table.
//   - data: map of column names to values.
//
// Returns:
//   - rows affected
//   - last insert ID
//   - error if any.
func InsertFromMap(ctx context.Context, dbctx *DBContext, tableName string, data map[string]interface{}) (int64, int64, error) {
	quotedTableName, err := safeMySQLIdent(tableName)
	if err != nil {
		return 0, 0, err
	}
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("empty data map")
	}

	columns := []string{}
	placeholders := []string{}
	values := []interface{}{}

	keys := make([]string, 0, len(data))
	for column := range data {
		keys = append(keys, column)
	}
	sort.Strings(keys)

	for _, column := range keys {
		quotedColumn, err := safeMySQLIdent(column)
		if err != nil {
			return 0, 0, err
		}
		columns = append(columns, quotedColumn)
		placeholders = append(placeholders, "?")
		values = append(values, data[column])
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quotedTableName, strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	if dbctx == nil {
		return 0, 0, fmt.Errorf("nil DB context provided, cannot execute the query")
	}

	result, err := execMySQLHelperOperation(ctx, dbctx, "insert_map", query, values...)
	if err != nil {
		return 0, 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		rowsAffected = 0
	}

	lastInsertId, err := result.LastInsertId()
	if err != nil {
		lastInsertId = 0
	}

	return rowsAffected, lastInsertId, nil
}

// UpdateFromMap updates rows in a table using a map of column-value pairs and a WHERE clause.
//
// Parameters:
//   - dbctx: DB context.
//   - tableName: name of the table.
//   - data: map of columns to new values.
//   - where: WHERE clause string (without "WHERE").
//   - params: parameters for WHERE clause.
//
// Returns:
//   - number of rows affected
//   - error if any.
func UpdateFromMap(ctx context.Context, dbctx *DBContext, tableName string, data map[string]interface{}, where string, params ...interface{}) (int64, error) {
	quotedTableName, err := safeMySQLIdent(tableName)
	if err != nil {
		return 0, err
	}
	if len(data) == 0 {
		return 0, fmt.Errorf("empty update map")
	}
	if strings.TrimSpace(where) == "" {
		return 0, fmt.Errorf("where clause cannot be empty")
	}

	setParts := []string{}
	values := []interface{}{}

	keys := make([]string, 0, len(data))
	for column := range data {
		keys = append(keys, column)
	}
	sort.Strings(keys)

	for _, column := range keys {
		quotedColumn, err := safeMySQLIdent(column)
		if err != nil {
			return 0, err
		}
		setParts = append(setParts, fmt.Sprintf("%s = ?", quotedColumn))
		values = append(values, data[column])
	}

	values = append(values, params...)

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", quotedTableName, strings.Join(setParts, ", "), where)

	if dbctx == nil {
		return 0, fmt.Errorf("nil DB context provided, cannot execute the query")
	}

	result, err := execMySQLHelperOperation(ctx, dbctx, "update_map", query, values...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		rowsAffected = 0
	}

	return rowsAffected, nil
}

// DeleteByPrimaryKey deletes a single record by primary key.
//
// Parameters:
//   - dbctx: DB context.
//   - tableName: name of the table.
//   - pkColumn: primary key column name.
//   - pkValue: value of the primary key.
//
// Returns:
//   - number of rows deleted
//   - error if any.
func DeleteByPrimaryKey(ctx context.Context, dbctx *DBContext, tableName, pkColumn string, pkValue interface{}) (int64, error) {
	quotedTableName, err := safeMySQLIdent(tableName)
	if err != nil {
		return 0, err
	}
	quotedPKColumn, err := safeMySQLIdent(pkColumn)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", quotedTableName, quotedPKColumn)

	if dbctx == nil {
		return 0, fmt.Errorf("nil DB context provided, cannot execute the query")
	}

	result, err := execMySQLHelperOperation(ctx, dbctx, "delete_primary_key", query, pkValue)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		rowsAffected = 0
	}

	return rowsAffected, nil
}

// SoftDeleteByPrimaryKey sets a soft-delete flag column to 1 for the given primary key.
//
// Parameters:
//   - dbctx: DB context.
//   - tableName: name of the table.
//   - deleteCol: soft delete column name.
//   - pkCol: primary key column name.
//   - value: value of the primary key.
//
// Returns:
//   - number of rows updated
//   - error if any.
func SoftDeleteByPrimaryKey(ctx context.Context, dbctx *DBContext, tableName, deleteCol, pkCol string, value interface{}) (int64, error) {
	quotedTableName, err := safeMySQLIdent(tableName)
	if err != nil {
		return 0, err
	}
	quotedDeleteCol, err := safeMySQLIdent(deleteCol)
	if err != nil {
		return 0, err
	}
	quotedPKCol, err := safeMySQLIdent(pkCol)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf("UPDATE %s SET %s = 1 WHERE %s = ?", quotedTableName, quotedDeleteCol, quotedPKCol)

	if dbctx == nil {
		return 0, fmt.Errorf("nil DB context provided, cannot execute the query")
	}

	result, err := execMySQLHelperOperation(ctx, dbctx, "soft_delete_primary_key", query, value)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		rowsAffected = 0
	}

	return rowsAffected, nil
}

// GetParameterizedInClause generates a named parameterized IN clause from a slice of values.
// Returns the SQL-safe IN clause and map of parameter keys to values.
//
// Parameters:
//   - columnName: name of the column for the IN clause.
//   - columnValueArray: slice of values to include in the clause.
//
// Returns:
//   - IN clause string
//   - map of named parameters
func GetParameterizedInClause[T any](columnName string, columnValueArray []T) (string, map[string]interface{}) {
	inClauseParams := make(map[string]interface{})
	var builder strings.Builder

	for index, value := range columnValueArray {
		key := fmt.Sprintf(":%s%d", columnName, index+1)
		builder.WriteString(key)
		builder.WriteString(",")
		inClauseParams[key] = value
	}

	inClauseString := strings.TrimSuffix(builder.String(), ",")
	return inClauseString, inClauseParams
}

// ConvertQueryAndNamedParams replaces named parameters in a query with positional placeholders (`?`)
// and returns the ordered list of parameters as expected by sql.DB.
//
// Parameters:
//   - query: SQL query with named parameters (e.g., :userId).
//   - params: one or more maps of named parameter values.
//
// Returns:
//   - modified query string
//   - slice of parameter values in correct order
func ConvertQueryAndNamedParams(query string, params ...map[string]interface{}) (string, []interface{}) {
	allParams := make(map[string]interface{})
	for _, oneParamDetails := range params {
		for k, v := range oneParamDetails {
			allParams[k] = v
		}
	}

	// Find all named parameters in the query
	re := regexp.MustCompile(`:[a-zA-Z0-9_]+`)
	matches := re.FindAllString(query, -1)

	paramValues := make([]interface{}, 0)

	for _, match := range matches {
		query = strings.Replace(query, match, "?", 1)
		paramValues = append(paramValues, allParams[match])
	}

	return query, paramValues
}

// GenerateHashKey creates a unique hash key based on the provided query and its arguments.
// It first extracts the keys from the argument map and sorts them. It then concatenates the
// query string and the sorted arguments to generate a hash key. The key is then hashed using
// SHA-1 and encoded to a base64 URL-safe string.
//
// Parameters:
//   - query: The SQL query string.
//   - args: A map of argument names to their values.
//
// Returns:
//   - A base64 URL-encoded string representation of the SHA-1 hash.
//   - An error if there's a failure during the hashing process.
func HashKey(query string, args map[string]interface{}) (string, error) {
	keys := make([]string, len(args))

	i := 0
	for k := range args {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	argStr := ""

	for i := range keys {
		argStr += fmt.Sprintf("%s:%v|", keys[i], args[keys[i]])
	}

	hashKey := []byte(fmt.Sprintf("%s%s", query, argStr))

	hasher := newHashWriter()
	_, err := hasher.Write(hashKey)

	if err != nil {
		return "", err
	}

	return base64.URLEncoding.EncodeToString(hasher.Sum(nil)), nil
}
