package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cshekharsharma/photon/utils/types"
	"github.com/pkg/errors"
)

var (
	helperPostgresConnector PostgresDbConnectorInterface = &PostgresDbConnector{}
	getReadColumnsHook                                   = func(rows *sql.Rows) ([]string, error) {
		return rows.Columns()
	}
	scanReadRowHook = func(rows *sql.Rows, dest ...any) error {
		return rows.Scan(dest...)
	}
	hashFprintfHook = fmt.Fprintf
)

type sqlCloser interface {
	Close() error
}

func closeSQLCloser(closer sqlCloser) {
	if err := closer.Close(); err != nil {
		return
	}
}

// ReadQueryInput mirrors your mysql helper pattern.
type ReadQueryInput struct {
	Query             string
	Params            []any
	CapitaliseColumns bool
}

// ExecuteReadQuery runs a read-only SELECT query.
// It returns []map[string]any where each value is typed (int64, bool, time.Time, string, nil, etc).
//
// NOTE: We do NOT prepare implicitly here. Preparing without using the prepared statement
// is wasteful and can break assumptions under PgBouncer. If you want explicit prepare,
// do it at the call site (or add an optional flag and execute via stmt).
func ExecuteReadQuery(ctx context.Context, dbctx *DBContext, queryInput ReadQueryInput) ([]map[string]any, error) {
	if dbctx == nil {
		return nil, fmt.Errorf("nil DB context provided, cannot execute the query")
	}

	// TODO(otel): span "postgres.helper.read" (queryInput.Query sanitized), args count, cluster, etc.
	rows, err := dbctx.Query(ctx, queryInput.Query, queryInput.Params...)
	if err != nil {
		return nil, err
	}
	if rows != nil {
		defer func() {
			_ = rows.Close()
		}()
	}

	cols, err := getReadColumnsHook(rows)
	if err != nil {
		return nil, err
	}

	out := make([]map[string]any, 0, 32)

	for rows.Next() {
		values := make([]any, len(cols))
		scanArgs := make([]any, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := scanReadRowHook(rows, scanArgs...); err != nil {
			return nil, err
		}

		rowMap := make(map[string]any, len(cols))
		for i, colName := range cols {
			if queryInput.CapitaliseColumns {
				colName = types.UCFirst(colName)
			}
			rowMap[colName] = normalizePgValue(values[i])
		}

		out = append(out, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

// normalizePgValue normalizes driver-returned values to friendlier Go types.
// - []byte (common under database/sql for text-like types) -> string
// - others left as-is
func normalizePgValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		return string(x)
	default:
		return x
	}
}

// ExecuteWriteQuery executes an INSERT/UPDATE/DELETE query.
// Note: Postgres generally does NOT support LastInsertId().
// Prefer `RETURNING id` and QueryRow/Scan for inserts that need IDs.
func ExecuteWriteQuery(ctx context.Context, dbctx *DBContext, query string, params []any) (int64, int64, error) {
	if dbctx == nil {
		return 0, 0, fmt.Errorf("nil DB context provided, cannot execute the query")
	}

	// TODO(otel): span "postgres.helper.write"
	result, err := dbctx.Exec(ctx, query, params...)
	if err != nil {
		return 0, 0, fmt.Errorf("error executing query: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, 0, fmt.Errorf("error fetching impacted rows: %w", err)
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		lastInsertID = 0
	}

	return rowsAffected, lastInsertID, nil
}

// MultiInsertFromStructsArray performs a bulk insert using a slice of structs,
// generating Postgres placeholders ($1..$N).
func MultiInsertFromStructsArray[T any](ctx context.Context, dbctx *DBContext, tableName string, data []T) (int64, error) {
	if len(data) == 0 {
		return 0, fmt.Errorf("input data array is empty")
	}
	if dbctx == nil {
		return 0, fmt.Errorf("nil DB context provided, cannot execute the query")
	}

	query, allValues, err := generateMultiInsertQueriesFromStructArray(tableName, data)
	if err != nil {
		return 0, errors.Wrap(err, "error in generating query from input data")
	}

	// TODO(otel): span "postgres.helper.multi_insert"
	result, err := dbctx.Exec(ctx, query, allValues...)
	if err != nil {
		return 0, fmt.Errorf("error executing insert: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("error fetching impacted rows: %w", err)
	}

	return rowsAffected, nil
}

func generateMultiInsertQueriesFromStructArray[T any](tableName string, data []T) (string, []any, error) {
	tbl, err := safeIdent(tableName)
	if err != nil {
		return "", nil, err
	}

	t, err := structTypeOf(data[0])
	if err != nil {
		return "", nil, err
	}

	fields, estimatedFieldCount := dbFields(t)
	quotedCols, err := quoteIdentifiers(fields)
	if err != nil {
		return "", nil, err
	}

	valueTuples := make([]string, 0, len(data))
	allValues := make([]any, 0, len(data)*estimatedFieldCount)
	argPos := 1

	for _, row := range data {
		fieldValues, fieldIsDefault, err := multiInsertRowValues(row, t, len(fields))
		if err != nil {
			return "", nil, err
		}

		tuple, values := multiInsertPlaceholders(fields, fieldValues, fieldIsDefault, &argPos)
		valueTuples = append(valueTuples, tuple)
		allValues = append(allValues, values...)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tbl, strings.Join(quotedCols, ", "), strings.Join(valueTuples, ", "))
	return query, allValues, nil
}

func structTypeOf(row any) (reflect.Type, error) {
	v := reflect.ValueOf(row)
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct input, got %s", v.Kind())
	}
	return v.Type(), nil
}

func dbFields(t reflect.Type) ([]string, int) {
	estimatedFieldCount := 0
	fieldsMap := make(map[string]struct{}, t.NumField())
	fields := make([]string, 0, t.NumField())

	for i := 0; i < t.NumField(); i++ {
		name, ok := dbTagName(t.Field(i))
		if !ok {
			continue
		}
		estimatedFieldCount++
		if _, exists := fieldsMap[name]; exists {
			continue
		}
		fieldsMap[name] = struct{}{}
		fields = append(fields, name)
	}

	return fields, estimatedFieldCount
}

func dbTagName(field reflect.StructField) (string, bool) {
	dbTag := field.Tag.Get("db")
	if dbTag == "" {
		return "", false
	}
	name := strings.Split(dbTag, ",")[0]
	return name, name != "-"
}

func quoteIdentifiers(fields []string) ([]string, error) {
	quotedCols := make([]string, 0, len(fields))
	for _, c := range fields {
		qc, err := safeIdent(c)
		if err != nil {
			return nil, err
		}
		quotedCols = append(quotedCols, qc)
	}
	return quotedCols, nil
}

func multiInsertRowValues(row any, t reflect.Type, fieldCount int) (map[string]any, map[string]bool, error) {
	sv := reflect.ValueOf(row)
	if sv.Kind() == reflect.Pointer {
		sv = sv.Elem()
	}
	if sv.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("expected struct input, got %s", sv.Kind())
	}

	fieldValues := make(map[string]any, fieldCount)
	fieldIsDefault := make(map[string]bool, fieldCount)
	for i := 0; i < t.NumField(); i++ {
		if err := setMultiInsertFieldValue(sv, t.Field(i), i, fieldValues, fieldIsDefault); err != nil {
			return nil, nil, err
		}
	}
	return fieldValues, fieldIsDefault, nil
}

func setMultiInsertFieldValue(
	sv reflect.Value,
	field reflect.StructField,
	index int,
	fieldValues map[string]any,
	fieldIsDefault map[string]bool,
) error {
	dbTag := field.Tag.Get("db")
	if dbTag == "" {
		return nil
	}

	tagParts := strings.Split(dbTag, ",")
	name := tagParts[0]
	if name == "-" {
		return nil
	}

	value := sv.Field(index).Interface()
	useDefault := slices.Contains(tagParts, "omitempty") && types.IsEmpty(value)
	if !useDefault && slices.Contains(tagParts, "marshaljson") && sv.Field(index).Kind() == reflect.Struct {
		jsonValue, err := json.Marshal(value)
		if err != nil {
			return err
		}
		value = string(jsonValue)
	}

	fieldValues[name] = value
	fieldIsDefault[name] = useDefault
	return nil
}

func multiInsertPlaceholders(fields []string, fieldValues map[string]any, fieldIsDefault map[string]bool, argPos *int) (string, []any) {
	placeholders := make([]string, 0, len(fields))
	values := make([]any, 0, len(fields))

	for _, name := range fields {
		if fieldIsDefault[name] {
			placeholders = append(placeholders, "DEFAULT")
			continue
		}
		placeholders = append(placeholders, "$"+strconv.Itoa(*argPos))
		(*argPos)++
		values = append(values, fieldValues[name])
	}

	return "(" + strings.Join(placeholders, ", ") + ")", values
}

// InsertFromStruct inserts one record into a table based on struct db tags.
// Uses Postgres placeholders ($1..$N).
func InsertFromStruct(ctx context.Context, dbctx *DBContext, tableName string, data any) (int64, int64, error) {
	if dbctx == nil {
		return 0, 0, fmt.Errorf("nil DB context provided, cannot execute the query")
	}

	tbl, err := safeIdent(tableName)
	if err != nil {
		return 0, 0, err
	}

	sv := reflect.ValueOf(data)
	if sv.Kind() == reflect.Pointer {
		sv = sv.Elem()
	}
	if sv.Kind() != reflect.Struct {
		return 0, 0, fmt.Errorf("expected struct input, got %s", sv.Kind())
	}

	t := sv.Type()
	cols := make([]string, 0)
	vals := make([]any, 0)
	phs := make([]string, 0)
	argPos := 1

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		dbTag := f.Tag.Get("db")
		if dbTag == "" {
			continue
		}

		tagParts := strings.Split(dbTag, ",")
		col := tagParts[0]
		if col == "-" {
			continue
		}

		value := sv.Field(i).Interface()
		omitempty := slices.Contains(tagParts, "omitempty")
		if omitempty && types.IsEmpty(value) {
			continue
		}

		if slices.Contains(tagParts, "marshaljson") && sv.Field(i).Kind() == reflect.Struct {
			jv, err := json.Marshal(value)
			if err != nil {
				return 0, 0, err
			}
			value = string(jv)
		}

		qc, err := safeIdent(col)
		if err != nil {
			return 0, 0, err
		}

		cols = append(cols, qc)
		phs = append(phs, "$"+strconv.Itoa(argPos))
		vals = append(vals, value)
		argPos++
	}

	if len(cols) == 0 {
		return 0, 0, fmt.Errorf("no insertable fields found (check db tags / omitempty)")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tbl, strings.Join(cols, ", "), strings.Join(phs, ", "))

	// TODO(otel): span "postgres.helper.insert_struct"
	res, execErr := dbctx.Exec(ctx, query, vals...)
	if execErr != nil {
		return 0, 0, execErr
	}

	rowsAffected, _ := res.RowsAffected()
	lastInsertID, err := res.LastInsertId()
	if err != nil {
		lastInsertID = 0
	}
	return rowsAffected, lastInsertID, nil
}

// InsertFromMap inserts a record using map[column]value.
// Uses Postgres placeholders ($1..$N).
func InsertFromMap(ctx context.Context, dbctx *DBContext, tableName string, data map[string]any) (int64, int64, error) {
	if dbctx == nil {
		return 0, 0, fmt.Errorf("nil DB context provided, cannot execute the query")
	}
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("empty data map")
	}

	tbl, err := safeIdent(tableName)
	if err != nil {
		return 0, 0, err
	}

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	cols := make([]string, 0, len(keys))
	phs := make([]string, 0, len(keys))
	vals := make([]any, 0, len(keys))

	for i, col := range keys {
		qc, err := safeIdent(col)
		if err != nil {
			return 0, 0, err
		}
		cols = append(cols, qc)
		phs = append(phs, "$"+strconv.Itoa(i+1))
		vals = append(vals, data[col])
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tbl, strings.Join(cols, ", "), strings.Join(phs, ", "))

	// TODO(otel): span "postgres.helper.insert_map"
	res, err := dbctx.Exec(ctx, query, vals...)
	if err != nil {
		return 0, 0, err
	}

	rowsAffected, _ := res.RowsAffected()
	lastInsertID, e := res.LastInsertId()
	if e != nil {
		lastInsertID = 0
	}
	return rowsAffected, lastInsertID, nil
}

// UpdateFromMap updates rows using map[column]value and a WHERE clause.
// whereClause may contain either:
// - Postgres placeholders ($N), OR
// - '?' placeholders (which will be converted to $N starting at the correct position)
func UpdateFromMap(ctx context.Context, dbctx *DBContext, tableName string, data map[string]any, whereClause string, params ...any) (int64, error) {
	if dbctx == nil {
		return 0, fmt.Errorf("nil DB context provided, cannot execute the query")
	}
	if len(data) == 0 {
		return 0, fmt.Errorf("empty update map")
	}

	tbl, err := safeIdent(tableName)
	if err != nil {
		return 0, err
	}

	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	setParts := make([]string, 0, len(keys))
	values := make([]any, 0, len(keys)+len(params))
	argPos := 1

	for _, col := range keys {
		qc, err := safeIdent(col)
		if err != nil {
			return 0, err
		}
		setParts = append(setParts, fmt.Sprintf("%s = $%d", qc, argPos))
		values = append(values, data[col])
		argPos++
	}

	convertedWhere, whereVals, err := convertWhereParamsToPg(whereClause, argPos, params...)
	if err != nil {
		return 0, err
	}
	values = append(values, whereVals...)

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tbl, strings.Join(setParts, ", "), convertedWhere)

	// TODO(otel): span "postgres.helper.update_map"
	res, err := dbctx.Exec(ctx, query, values...)
	if err != nil {
		return 0, err
	}

	rowsAffected, _ := res.RowsAffected()
	return rowsAffected, nil
}

func DeleteByPrimaryKey(ctx context.Context, dbctx *DBContext, tableName, pkColumn string, pkValue any) (int64, error) {
	if dbctx == nil {
		return 0, fmt.Errorf("nil DB context provided, cannot execute the query")
	}

	tbl, err := safeIdent(tableName)
	if err != nil {
		return 0, err
	}
	pk, err := safeIdent(pkColumn)
	if err != nil {
		return 0, err
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE %s = $1", tbl, pk)

	// TODO(otel): span "postgres.helper.delete_pk"
	res, err := dbctx.Exec(ctx, query, pkValue)
	if err != nil {
		return 0, err
	}

	rowsAffected, _ := res.RowsAffected()
	return rowsAffected, nil
}

func SoftDeleteByPrimaryKey(ctx context.Context, dbctx *DBContext, tableName, deleteCol, pkCol string, value any) (int64, error) {
	if dbctx == nil {
		return 0, fmt.Errorf("nil DB context provided, cannot execute the query")
	}

	tbl, err := safeIdent(tableName)
	if err != nil {
		return 0, err
	}
	del, err := safeIdent(deleteCol)
	if err != nil {
		return 0, err
	}
	pk, err := safeIdent(pkCol)
	if err != nil {
		return 0, err
	}

	// Keep bool default; if your schema uses int flags, pass 1 instead of true.
	query := fmt.Sprintf("UPDATE %s SET %s = $1 WHERE %s = $2", tbl, del, pk)

	// TODO(otel): span "postgres.helper.soft_delete"
	res, err := dbctx.Exec(ctx, query, true, value)
	if err != nil {
		return 0, err
	}

	rowsAffected, _ := res.RowsAffected()
	return rowsAffected, nil
}

// GetParameterizedInClause generates a named IN clause like ":id1,:id2,..."
// and returns a map of placeholders to values.
// This keeps your mysql helper API style.
func GetParameterizedInClause[T any](columnName string, columnValueArray []T) (string, map[string]any) {
	inClauseParams := make(map[string]any, len(columnValueArray))
	var b strings.Builder

	for i, v := range columnValueArray {
		key := fmt.Sprintf(":%s%d", columnName, i+1)
		b.WriteString(key)
		b.WriteString(",")
		inClauseParams[key] = v
	}

	inClauseString := strings.TrimSuffix(b.String(), ",")
	return inClauseString, inClauseParams
}

// ConvertQueryAndNamedParams converts ":name" tokens into Postgres positional params ($1,$2,...)
// and returns ordered param values.
//
// This implementation is robust against:
// - Postgres casts like "col::int" (won't treat ::int as a param)
// - Single-quoted strings: 'text :not_a_param'
// - Dollar-quoted strings: $$ ... :not_a_param ... $$
//
// Constraints (intentional for safety):
// - Named tokens must be [A-Za-z0-9_]+ and be prefixed by single ':'
// - The params maps must use keys like ":name" (same as your mysql helper)
func ConvertQueryAndNamedParams(query string, params ...map[string]any) (string, []any) {
	converter := newNamedParamConverter(query, mergeNamedParams(params...))
	converter.convert()
	return converter.out.String(), converter.ordered
}

type namedParamConverter struct {
	query         string
	params        map[string]any
	out           strings.Builder
	ordered       []any
	seen          map[string]int
	argPos        int
	index         int
	inSingleQuote bool
	inDollarQuote bool
	dollarTag     string
}

func newNamedParamConverter(query string, params map[string]any) *namedParamConverter {
	converter := &namedParamConverter{
		query:   query,
		params:  params,
		ordered: make([]any, 0, 8),
		seen:    make(map[string]int, 16),
		argPos:  1,
	}
	converter.out.Grow(len(query) + 16)
	return converter
}

func mergeNamedParams(params ...map[string]any) map[string]any {
	allParams := make(map[string]any)
	for _, mp := range params {
		for k, v := range mp {
			allParams[k] = v
		}
	}
	return allParams
}

func (c *namedParamConverter) convert() {
	for c.index < len(c.query) {
		if c.handleDollarQuote() || c.handleSingleQuote() || c.copyQuotedByte() || c.handleNamedParam() {
			continue
		}
		c.out.WriteByte(c.query[c.index])
		c.index++
	}
}

func (c *namedParamConverter) handleDollarQuote() bool {
	if c.inSingleQuote || c.query[c.index] != '$' {
		return false
	}
	if c.inDollarQuote && c.dollarTag != "" && strings.HasPrefix(c.query[c.index:], c.dollarTag) {
		c.inDollarQuote = false
		c.out.WriteString(c.dollarTag)
		c.index += len(c.dollarTag)
		c.dollarTag = ""
		return true
	}
	if c.inDollarQuote {
		return false
	}

	tag, ok := parseDollarTag(c.query[c.index:])
	if !ok {
		return false
	}
	c.inDollarQuote = true
	c.dollarTag = tag
	c.out.WriteString(tag)
	c.index += len(tag)
	return true
}

func (c *namedParamConverter) handleSingleQuote() bool {
	if c.inDollarQuote || c.query[c.index] != '\'' {
		return false
	}

	c.out.WriteByte(c.query[c.index])
	if c.inSingleQuote && c.index+1 < len(c.query) && c.query[c.index+1] == '\'' {
		c.out.WriteByte(c.query[c.index+1])
		c.index += 2
		return true
	}
	c.inSingleQuote = !c.inSingleQuote
	c.index++
	return true
}

func (c *namedParamConverter) copyQuotedByte() bool {
	if !c.inSingleQuote && !c.inDollarQuote {
		return false
	}
	c.out.WriteByte(c.query[c.index])
	c.index++
	return true
}

func (c *namedParamConverter) handleNamedParam() bool {
	if c.query[c.index] != ':' {
		return false
	}
	if c.copyPostgresCast() {
		return true
	}

	end := namedParamEnd(c.query, c.index+1)
	if end == c.index+1 {
		c.out.WriteByte(':')
		c.index++
		return true
	}

	c.writeNamedParam(c.query[c.index:end])
	c.index = end
	return true
}

func (c *namedParamConverter) copyPostgresCast() bool {
	if c.index+1 >= len(c.query) || c.query[c.index+1] != ':' {
		return false
	}
	c.out.WriteString("::")
	c.index += 2
	return true
}

func (c *namedParamConverter) writeNamedParam(token string) {
	if pos, ok := c.seen[token]; ok {
		c.out.WriteString("$" + strconv.Itoa(pos))
		return
	}

	c.seen[token] = c.argPos
	c.out.WriteString("$" + strconv.Itoa(c.argPos))
	c.ordered = append(c.ordered, c.params[token])
	c.argPos++
}

func namedParamEnd(query string, start int) int {
	i := start
	for i < len(query) && isNamedParamChar(query[i]) {
		i++
	}
	return i
}

func isNamedParamChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '_'
}

// HashKey creates a deterministic hash for query+args map (same intent as your mysql helper).
func HashKey(query string, args map[string]any) (string, error) {
	keys := make([]string, 0, len(args))
	for k := range args {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(query)
	for _, k := range keys {
		b.WriteString(k)
		b.WriteString(":")
		if _, err := hashFprintfHook(&b, "%v", args[k]); err != nil {
			return "", err
		}
		b.WriteString("|")
	}

	sum := sha256.Sum256([]byte(b.String()))
	return base64.URLEncoding.EncodeToString(sum[:]), nil
}

// --- internals ---

// safeIdent validates and quotes identifiers (table/column).
// Identifiers cannot be bound as query params, so we must ensure they are trusted.
// This helper:
// - allows schema.table or table.column style identifiers
// - ensures only [a-zA-Z0-9_\.] characters are used
// - quotes each segment with double quotes (Postgres standard)
//
// NOTE: Use ONLY for identifiers, never for expressions.
func safeIdent(ident string) (string, error) {
	ident = strings.TrimSpace(ident)
	if ident == "" {
		return "", fmt.Errorf("empty identifier")
	}

	for _, r := range ident {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '_' || r == '.' {
			continue
		}
		return "", fmt.Errorf("invalid identifier %q", ident)
	}

	parts := strings.Split(ident, ".")
	for i, p := range parts {
		if p == "" {
			return "", fmt.Errorf("invalid identifier %q", ident)
		}
		parts[i] = `"` + p + `"`
	}
	return strings.Join(parts, "."), nil
}

// convertWhereParamsToPg converts a WHERE clause using '?' placeholders into Postgres style,
// starting at a given placeholder index.
//
// It validates that number of '?' matches len(params).
func convertWhereParamsToPg(where string, start int, params ...any) (string, []any, error) {
	if !strings.Contains(where, "?") {
		// assume caller already provided $ placeholders
		return where, params, nil
	}

	count := strings.Count(where, "?")
	if count != len(params) {
		return "", nil, fmt.Errorf("postgres: where placeholder mismatch: found %d '?' but got %d params", count, len(params))
	}

	out := where
	pos := start
	for strings.Contains(out, "?") {
		out = strings.Replace(out, "?", "$"+strconv.Itoa(pos), 1)
		pos++
	}
	return out, params, nil
}

// parseDollarTag detects $...$ tag at the beginning of s.
// Returns (tag, true) if found.
// Examples: "$$", "$tag$"
func parseDollarTag(s string) (string, bool) {
	if len(s) < 2 || s[0] != '$' {
		return "", false
	}
	// find next '$'
	j := 1
	for j < len(s) && s[j] != '$' {
		// tag chars: letters, digits, underscore
		c := s[j]
		if (c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') ||
			c == '_' {
			j++
			continue
		}
		return "", false
	}
	if j < len(s) && s[j] == '$' {
		return s[:j+1], true
	}
	return "", false
}

// Optional small helper for callers.
func nowUTC() time.Time { return time.Now().UTC() }
