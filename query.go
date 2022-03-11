package sqlair

import (
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"unicode"

	sreflect "github.com/SimonRichardson/sqlair/reflect"
	"github.com/pkg/errors"
)

const (
	// AliasPrefix is a prefix used to decode the mappings from column name.
	AliasPrefix = "_pfx_"
	// AliasSeparator is a separator used to decode the mappings from column
	// name.
	AliasSeparator = "_sfx_"
)

// Hook is used to analyze the queries that are being queried.
type Hook func(string)

type Querier struct {
	reflect   *sreflect.ReflectCache
	hook      Hook
	stmtCache *statementCache
}

// NewQuerier creates a new querier for selecting queries.
func NewQuerier() *Querier {
	return &Querier{
		reflect:   sreflect.NewReflectCache(),
		hook:      func(s string) {},
		stmtCache: newStatementCache(),
	}
}

// Hook assigns the hook to the querier. Each hook call precedes the actual
// query and outputs the compiled statement that's actually used in a query or
// exec.
//
func (q *Querier) Hook(hook Hook) {
	q.hook = hook
}

// ForOne creates a query for a set of given types. The values will be populated
// from the SQL query once executed.
//
// It should be noted that the query can be cached and the query can be called
// multiple times.
func (q *Querier) ForOne(values ...interface{}) (Query, error) {
	entities, err := q.reflectValues(values...)
	if err != nil {
		return Query{}, nil
	}
	query := Query{
		entities:  entities,
		hook:      q.hook,
		stmtCache: q.stmtCache,
		reflect:   q.reflect,
	}
	if len(values) == 0 {
		query.executePlan = query.defaultScan
		return query, nil
	}

	// We can expect that all entity types are homogeneous as there is a guard
	// in reflectValues method.
	switch entities[0].Kind() {
	case reflect.Struct:
		structs := make([]sreflect.ReflectStruct, len(values))
		for i, entity := range entities {
			structs[i] = entity.(sreflect.ReflectStruct)
		}

		query.executePlan = func(tx *sql.Tx, stmt string, args []interface{}) error {
			return query.structScan(tx, stmt, args, structs)
		}

	case reflect.Map:
		if len(values) > 1 {
			return Query{}, errors.Errorf("expected one map for query, got %d", len(values))
		}
		query.executePlan = func(tx *sql.Tx, stmt string, args []interface{}) error {
			return query.mapScan(tx, stmt, args, entities[0].(sreflect.ReflectValue))
		}

	default:
		query.executePlan = query.defaultScan
	}
	return query, nil
}

type reflectSlice struct {
	slice   sreflect.ReflectValue
	element sreflect.ReflectStruct
}

// ForMany creates a query based on the slice input. The values will be
// populated from the SQL query once executed.
//
// It should be noted that the query can be cached and the query can be called
// multiple times.
func (q *Querier) ForMany(values ...interface{}) (Query, error) {
	if len(values) == 0 {
		return Query{}, errors.Errorf("expected at least one argument")
	}

	entities, err := q.reflectValues(values...)
	if err != nil {
		return Query{}, nil
	}

	query := Query{
		entities:  entities,
		hook:      q.hook,
		stmtCache: q.stmtCache,
		reflect:   q.reflect,
	}

	refSlice := make([]reflectSlice, len(entities))
	for i, entity := range entities {
		switch entity.Kind() {
		case reflect.Slice:
			// This isn't nice at all, but we need to locate the base type of the
			// slice so we can iterate over it.
			refValue := entity.(sreflect.ReflectValue)
			base := refValue.Value.Type().Elem()
			virtual := reflect.New(base)

			// Grab the base type reflection.
			element, err := q.reflect.Reflect(virtual.Interface())
			if err != nil {
				return Query{}, errors.Errorf("expected slice but got %q", entity.Kind())
			}
			elementRefStruct, ok := element.(sreflect.ReflectStruct)
			if !ok {
				return Query{}, errors.Errorf("expected slice T to be struct")
			}

			refSlice[i] = reflectSlice{
				slice:   refValue,
				element: elementRefStruct,
			}

		default:
			return Query{}, errors.Errorf("expected slice but got %q", entity.Kind())
		}
	}

	query.executePlan = func(tx *sql.Tx, stmt string, args []interface{}) error {
		return query.sliceStructScan(tx, stmt, args, refSlice)
	}

	return query, nil
}

// Exec executes a query that doesn't return rows. Named arguments can be
// used within the statement.
func (q *Querier) Exec(tx *sql.Tx, stmt string, args ...interface{}) (sql.Result, error) {
	namedArgs, err := constructNamedArguments(stmt, args)
	if err != nil {
		return nil, errors.Wrap(err, "constructing named arguments")
	}

	if q.hook != nil {
		q.hook(stmt)
	}

	return tx.Exec(stmt, namedArgs...)
}

func (q *Querier) reflectValues(values ...interface{}) ([]sreflect.ReflectInfo, error) {
	entities := make([]sreflect.ReflectInfo, len(values))
	for i, value := range values {
		var err error

		if entities[i], err = q.reflect.Reflect(value); err != nil {
			return nil, errors.Wrap(err, "reflect")
		}

		// Ensure that all the types are the same. This is a current
		// restriction to reduce complications later on. Given enough time and
		// energy we can implement this at a later date.
		if i > 1 && entities[i-1].Kind() != entities[i].Kind() {
			return nil, errors.Errorf("expected all input values to be of the same kind %q, got %q", entities[i-1].Kind(), entities[i].Kind())
		}
	}
	return entities, nil
}

// Copy returns a new Querier with a new hook and statement cache, but keeping
// the existing reflect cache..
func (q *Querier) Copy() *Querier {
	return &Querier{
		reflect:   q.reflect,
		hook:      func(s string) {},
		stmtCache: newStatementCache(),
	}
}

type Query struct {
	entities    []sreflect.ReflectInfo
	hook        Hook
	executePlan func(*sql.Tx, string, []interface{}) error
	stmtCache   *statementCache
	reflect     *sreflect.ReflectCache
}

// Query executes a query that returns rows. Query will attempt to parse the
// statement for Records or NamedArguments
//
// Records
//
// Records are a way to represent the fields of a type without having to
// manually write them out or resorting to the wildcard *, which can be
// classified as an anti-pattern. The type found within the expression matches
// the fields and a warning is thrown if there are fields that are found not
// valid.
//
// Record expressions can be written in the following ways, where Person
// represents a struct type and must have field tags to express the intention.
//
//  type Person struct {
//  	Name string `db:"name"`
//  	Age  int    `db:"age"`
//  }
//
// In the simplist form, a record will be the name of the type "Person" in this
// example, surrounded by matching curly brackets: "{Person}". The type name is
// case sensitive, but space around the brackets isn't.
// The following expression will not prefix any fields with any additional
// information. The usage of this expression is for non-joining statements
// (simple "select from" query).
//
//  SELECT {Person} FROM people;
//
// Expands to become:
//
//  SELECT name, age FROM people;
//
// For more complex join examples where multiple types are to be expressed
// inside the statement, then a prefix with a field name can be added to
// help extract direct field values.
//
//  type Location struct {
//  	City string `db:"city"`
//  }
//
//  SELECT {people.* INTO Person}, {location.* INTO Location} FROM people INNER JOIN location WHERE location.id=:loc_id AND people.name=:name;
//
// Expands to become:
//
//  SELECT people.age, people.name, location.city FROM people INNER JOIN location ON people.location=location.id WHERE location.id=:loc_id AND people.name=:name
//
// Named Arguments
//
// Named arguments allow the expressing of fields via the name, rather than
// by a arbitrary number of placeholders (?). This helps ensure that the
// mistakes on matching values of a query to the arguments are less problematic.
//
// Named arguments can have a prefix of ":", "@" or "$" with alpha numeric
// characters there after, but "?" must only container numeric characters
// succeeding it.
//
// The arguments passed into a query can either be a map[string]interface{} or
// a type with fields tagged with the db: prefix.
//
//  querier.Query(tx, "SELECT {Person} FROM people WHERE name=:name;", map[string]interface{}{
//  	"name": "fred",
//  }
//
// See https://www.sqlite.org/c3ref/bind_blob.html for more information on
// named arguments in SQLite.
func (q Query) Query(tx *sql.Tx, stmt string, args ...interface{}) error {
	namedArgs, err := constructNamedArguments(stmt, args)
	if err != nil {
		return errors.Wrap(err, "constructing named arguments")
	}
	return q.executePlan(tx, stmt, namedArgs)
}

func (q Query) defaultScan(tx *sql.Tx, stmt string, args []interface{}) error {
	rows, columns, err := q.query(tx, stmt, args)
	if err != nil {
		return err
	}
	defer rows.Close()

	if len(columns) != len(q.entities) {
		return errors.Errorf("number of entities does not match column length %d, got %d", len(columns), len(q.entities))
	}

	columnar := make([]interface{}, len(columns))
	for i := range columns {
		if _, ok := q.entities[i].(sreflect.ReflectStruct); ok {
			return errors.Errorf("mixed entities not supported")
		}

		refValue := q.entities[i].(sreflect.ReflectValue)
		columnar[i] = refValue.Value.Addr().Interface()
	}

	return q.scanOne(rows, columnar)
}

func (q Query) mapScan(tx *sql.Tx, stmt string, args []interface{}, entity sreflect.ReflectValue) error {
	rows, columns, err := q.query(tx, stmt, args)
	if err != nil {
		return err
	}
	defer rows.Close()

	columnar := make([]interface{}, len(columns))
	for i, column := range columns {
		columnar[i] = zeroScanType(column.DatabaseTypeName())
	}
	if err := q.scanOne(rows, columnar); err != nil {
		return err
	}

	for i, column := range columns {
		columnName := column.Name()
		colRef := reflect.ValueOf(columnName)
		entity.Value.SetMapIndex(colRef, reflect.Indirect(reflect.ValueOf(columnar[i])))
	}

	return nil
}

func zeroScanType(t string) interface{} {
	switch strings.ToUpper(t) {
	case "TEXT":
		var a string
		return &a
	case "INTEGER":
		var a int64
		return &a
	case "BOOL":
		var a bool
		return &a
	case "REAL", "NUMERIC":
		var a float64
		return &a
	case "BLOB":
		var a []byte
		return &a
	default:
		panic("unexpected type: " + t)
	}
}

func compileStatement(stmt string, entities []sreflect.ReflectStruct) (string, []recordBinding, error) {
	var fields []recordBinding
	if offset := indexOfRecordArgs(stmt); offset >= 0 {
		var err error
		fields, err = parseRecords(stmt, offset)
		if err != nil {
			return "", nil, err
		}

		// Workout if any of the entities have overlapping fields.
		intersections := fieldIntersections(entities)

		stmt, err = expandRecords(stmt, fields, entities, intersections)
		if err != nil {
			return "", nil, err
		}
	}
	return stmt, fields, nil
}

func (q Query) structScan(tx *sql.Tx, stmt string, args []interface{}, entities []sreflect.ReflectStruct) error {
	var (
		compiledStmt string
		fields       []recordBinding
	)
	if cached, ok := q.stmtCache.Get(stmt); ok {
		compiledStmt = cached.stmt
		fields = cached.fields
	} else {
		var err error
		compiledStmt, fields, err = compileStatement(stmt, entities)
		if err != nil {
			return err
		}
	}

	rows, columns, err := q.query(tx, compiledStmt, args)
	if err != nil {
		return err
	}
	defer rows.Close()

	columnar, err := q.structMapping(columns, entities, fields)
	if err != nil {
		return err
	}

	if err := q.scanOne(rows, columnar); err != nil {
		return err
	}

	// Only cache the statement if it differs from the original.
	if stmt != compiledStmt {
		q.stmtCache.Set(stmt, cachedStmt{
			stmt:   compiledStmt,
			fields: fields,
		})
	}

	return nil
}

func (q Query) sliceStructScan(tx *sql.Tx, stmt string, args []interface{}, slice []reflectSlice) error {
	elements := make([]sreflect.ReflectStruct, len(slice))
	for i, ref := range slice {
		elements[i] = ref.element
	}
	compiledStmt, fields, err := compileStatement(stmt, elements)
	if err != nil {
		return err
	}

	rows, columns, err := q.query(tx, compiledStmt, args)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		refStructs := make([]sreflect.ReflectStruct, len(elements))
		for i, element := range elements {
			base := reflect.New(element.Value.Type())
			refInfo, err := q.reflect.Reflect(base.Interface())
			if err != nil {
				return err
			}
			refStruct, ok := refInfo.(sreflect.ReflectStruct)
			if !ok {
				return errors.Errorf("expected struct found %q", refInfo.Kind())
			}
			refStructs[i] = refStruct
		}

		columnar, err := q.structMapping(columns, elements, fields)
		if err != nil {
			return err
		}

		if err := rows.Scan(columnar...); err != nil {
			return err
		}

		for k, refSlice := range slice {
			sliceVal := refSlice.slice.Value
			sliceVal.Set(reflect.Append(sliceVal, elements[k].Value))
		}
	}
	return rows.Err()
}

func (q Query) structMapping(columns []*sql.ColumnType, entities []sreflect.ReflectStruct, fields []recordBinding) ([]interface{}, error) {
	// Traverse the entities available, this is where it becomes very difficult
	// for use. As the sql library doesn't provide the namespaced columns for
	// us to inspect, so if you have overlapping column names it becomes hard
	// to know where to locate that information, without a SQL AST.
	columnar := make([]interface{}, len(columns))
	for i, column := range columns {
		columnName := column.Name()

		var prefix string
		if strings.HasPrefix(columnName, AliasPrefix) {
			parts := strings.Split(columnName[len(AliasPrefix):], AliasSeparator)
			prefix = parts[0]
			columnName = parts[1]
		}

		var found bool
		for _, entity := range entities {
			field, ok := entity.Fields[columnName]
			if !ok {
				continue
			}
			if prefix != "" {
				var bindingFound bool
				for _, binding := range fields {
					if binding.name == entity.Name && binding.prefix == prefix {
						bindingFound = true
						break
					}
				}
				if !bindingFound {
					continue
				}
			}

			columnar[i] = field.Value.Addr().Interface()
			found = true
			break
		}
		if !found {
			return nil, errors.Errorf("missing destination name %q in types %v", column.Name(), entityNames(q.entities))
		}
	}
	return columnar, nil
}

func (q Query) query(tx *sql.Tx, stmt string, args []interface{}) (*sql.Rows, []*sql.ColumnType, error) {
	// Call the hook, before making the query.
	if q.hook != nil {
		q.hook(stmt)
	}

	rows, err := tx.Query(stmt, args...)
	if err != nil {
		return nil, nil, err
	}

	// Grab the columns of the rows returned.
	columns, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return nil, nil, err
	}
	return rows, columns, nil
}

func (q Query) scanOne(rows *sql.Rows, args []interface{}) error {
	for rows.Next() {
		if err := rows.Scan(args...); err != nil {
			return err
		}
	}

	return rows.Err()
}

func entityNames(entities []sreflect.ReflectInfo) []string {
	var names []string
	for _, entity := range entities {
		if rs, ok := entity.(sreflect.ReflectStruct); ok {
			names = append(names, rs.Name)
		}
	}
	return names
}

type bindCharPredicate func(rune) bool

func alphaNumeric(a rune) bool {
	return unicode.IsLetter(a) || unicode.IsDigit(a) || unicode.IsNumber(a) || a == '_'
}

func numeric(a rune) bool {
	return unicode.IsDigit(a) || unicode.IsNumber(a)
}

var prefixes = map[rune]bindCharPredicate{
	':': alphaNumeric,
	'@': alphaNumeric,
	'$': alphaNumeric,
	'?': numeric,
}

// indexOfInputNamedArgs returns the potential starting index of a named argument
// within the statement contains the named args prefix.
// This can return a false positives.
func indexOfInputNamedArgs(stmt string) int {
	// Let's be explicit that we've found something, we could just use the
	// res to see if it's moved, but that's more cryptic.
	var found bool
	res := len(stmt) + 1
	for prefix := range prefixes {
		if index := strings.IndexRune(stmt, prefix); index >= 0 && index < res {
			res = index
			found = true
		}
	}
	if found {
		return res
	}
	return -1
}

type nameBinding struct {
	prefix rune
	name   string
}

// parseNames extracts the named arguments from a given statement.
//
// Spec: https://www.sqlite.org/c3ref/bind_blob.html
//
// Literals may be replaced by a parameter that matches one of following
// templates:
//  - ?
//  - ?NNN
//  - :VVV
//  - @VVV
//  - $VVV
// In the templates above:
//  - NNN represents an integer literal
//  - VVV represents an alphanumeric identifier.
//
func parseNames(stmt string, offset int) ([]nameBinding, error) {
	var names []nameBinding

	// Use the offset to jump ahead of the statement.
	for i := offset; i < len(stmt); i++ {
		r := rune(stmt[i])
		if predicate, ok := prefixes[r]; ok {
			// We need to special case empty '?' as they're valid, but are not
			// valid binds.
			if r == '?' && i+1 < len(stmt) && isNameTerminator(rune(stmt[i+1])) {
				continue
			}

			// Consume the following runes, until you locate a breaking value.
			var name string
			for i = i + 1; i < len(stmt); i++ {
				char := rune(stmt[i])

				if predicate(char) {
					name += string(char)
					continue
				}
				if isNameTerminator(char) {
					break
				}
				return nil, errors.Errorf("unexpected named argument found in statement %q", stmt)
			}
			names = append(names, nameBinding{
				prefix: r,
				name:   name,
			})

			// Locate the index of the next name. We use this to skip over
			// any complexities.
			if i >= len(stmt) {
				// We're done processing the stmt.
				break
			}
			index := indexOfInputNamedArgs(stmt[i:])
			if index == -1 {
				// No additional names, skip.
				break
			}
			// We want to reduce the index by 1, so that we also pick up the
			// prefix, otherwise we skip over it.
			i += (index - 1)
		}
	}
	sort.Slice(names, func(i int, j int) bool {
		return names[i].name < names[j].name
	})
	return names, nil
}

func isNameTerminator(a rune) bool {
	return unicode.IsSpace(a) || a == ',' || a == ';' || a == '=' || a == ')'
}

func constructInputNamedArgs(arg interface{}, names []nameBinding) ([]sql.NamedArg, error) {
	t := reflect.TypeOf(arg)
	k := t.Kind()
	switch {
	case k == reflect.Map && t.Key().Kind() == reflect.String:
		m, ok := convertMapStringInterface(arg)
		if !ok {
			return nil, errors.Errorf("map type: %T not supported", arg)
		}
		nameValues := make([]sql.NamedArg, len(names))
		for k, name := range names {
			if value, ok := m[name.name]; ok {
				nameValues[k] = sql.Named(name.name, value)
				continue
			}

			return nil, errors.Errorf("key %q missing from map", name.name)
		}
		return nameValues, nil

	case k == reflect.Array || k == reflect.Slice:
		return nil, errors.Errorf("%q not supported", k.String())
	default:
		ref, err := sreflect.Reflect(reflect.ValueOf(arg))
		if err != nil {
			return nil, err
		}
		refStruct, ok := ref.(sreflect.ReflectStruct)
		if !ok {
			return nil, errors.Errorf("%q not supported", k)
		}

		nameValues := make([]sql.NamedArg, len(names))
		for k, name := range names {
			if field, ok := refStruct.Fields[name.name]; ok {
				fieldValue := field.Value.Interface()
				nameValues[k] = sql.Named(name.name, fieldValue)
				continue
			}

			return nil, errors.Errorf("field %q missing from type %T", name.name, arg)
		}

		return nameValues, nil
	}
}

func constructNamedArguments(stmt string, args []interface{}) ([]interface{}, error) {
	var names []nameBinding
	if offset := indexOfInputNamedArgs(stmt); offset >= 0 {
		var err error
		if names, err = parseNames(stmt, offset); err != nil {
			return nil, err
		}
	}

	// Ensure we have arguments if we have names.
	if len(args) == 0 && len(names) > 0 {
		return nil, errors.Errorf("expected arguments for named parameters")
	}

	var inputs []sql.NamedArg
	if len(names) > 0 && len(args) >= 1 {
		// TODO: We may have colliding sql.NamedArgs already sent in, if that's
		// the case we should document whom wins.

		// Select the first argument and check if it's a map or struct.
		var err error
		if inputs, err = constructInputNamedArgs(args[0], names); err != nil {
			return nil, err
		}
		// Drop the first argument, as that's used for named arguments.
		args = args[1:]
	}

	// Put the named arguments at the end of the query.
	for _, input := range inputs {
		args = append(args, input)
	}
	return args, nil
}

// convertMapStringInterface attempts to convert v to map[string]interface{}.
// Unlike v.(map[string]interface{}), this function works on named types that
// are convertible to map[string]interface{} as well.
func convertMapStringInterface(v interface{}) (map[string]interface{}, bool) {
	var m map[string]interface{}
	mType := reflect.TypeOf(m)
	t := reflect.TypeOf(v)
	if !t.ConvertibleTo(mType) {
		return nil, false
	}
	return reflect.ValueOf(v).Convert(mType).Interface().(map[string]interface{}), true
}

// indexOfRecordArgs returns the potential starting index of a record argument
// if the statement contains the record args offset position.
func indexOfRecordArgs(stmt string) int {
	return strings.IndexRune(stmt, '&')
}

type recordBinding struct {
	name       string
	prefix     string
	fields     map[string]struct{}
	wildcard   bool
	start, end int
}

func (f recordBinding) translate(expantion int) int {
	return expantion - (f.end - f.start)
}

func parseRecords(stmt string, offset int) ([]recordBinding, error) {
	var records []recordBinding
	for i := offset; i < len(stmt); i++ {
		r := rune(stmt[i])
		if r != '&' {
			return records, nil
		}

		// Parse the Record syntax `<table>.<column|*> AS &<entity path>`

		// The first part of the record pinding is to select the entity path.
		fmt.Println("??")
		entityPath, err := parseRecordPath(stmt, i+1)
		if err != nil && err != ErrTooMany {
			return nil, err
		}
		fmt.Println("???")

		// Reverse the look to ensure we have ` AS ` selector.
		var (
			selector string
			offset   int
		)
	inner:
		for k := i - 1; k >= 0; k-- {
			char := rune(stmt[k])
			switch {
			case unicode.IsSpace(char):
				if len(selector) > 0 {
					break inner
				}
			case unicode.IsLetter(char):
				selector = string(char) + selector
			default:
				return nil, errors.Errorf("expected selector")
			}
			offset = k
		}
		switch strings.ToLower(strings.TrimSpace(selector)) {
		case "as":
		default:
			return nil, errors.Errorf("expected AS selector, got: %q", selector)
		}

		// Reverse the look to ensure we the `<table>.<column|*>`.

		prior := 0
		fmt.Println(">>", offset)
		tablePath, err := parseRecordPath(stmt, prior+1)
		if err != nil && err != ErrTooMany {
			return nil, err
		}
		fmt.Println(entityPath, tablePath)
	}
	return records, nil
}

func expandRecords(stmt string, records []recordBinding, entities []sreflect.ReflectStruct, intersections map[string]map[string]struct{}) (string, error) {
	var offset int
	for _, record := range records {

		var found bool
		for _, entity := range entities {
			if record.name != entity.Name {
				continue
			}

			// Locate any field intersections from the records that's been
			// pre-computed.
			entityInter := intersections[entity.Name]

			var names []string
			if record.wildcard {
				// If we're wildcarded, just grab all the names.
				for name := range entity.Fields {
					names = append(names, constructFieldNameAlias(name, record, entityInter))
				}
			} else {
				// If we're not wildcarded, go through all the binding fields
				// and locate the entity field for the Record.
				for name := range record.fields {
					if _, ok := entity.Fields[name]; !ok {
						return "", errors.Errorf("field %q not found in entity %q", name, entity.Name)
					}
					names = append(names, constructFieldNameAlias(name, record, entityInter))
				}
			}

			if len(names) == 0 {
				return "", errors.Errorf("no fields found in record %q expression", entity.Name)
			}
			sort.Strings(names)
			recordList := strings.Join(names, ", ")
			stmt = stmt[:offset+record.start] + recordList + stmt[offset+record.end:]

			// Translate the offset to take into account the new expantions.
			offset += record.translate(len(recordList))

			found = true
			break
		}

		if !found {
			return "", errors.Errorf("no entity found with the name %q", record.name)
		}
	}

	return stmt, nil
}

func constructFieldNameAlias(name string, record recordBinding, intersection map[string]struct{}) string {
	if record.prefix == "" {
		return name
	}
	var alias string
	if _, ok := intersection[name]; ok {
		alias = " AS " + AliasPrefix + record.prefix + AliasSeparator + name
	}
	return record.prefix + "." + name + alias
}

func fieldIntersections(entities []sreflect.ReflectStruct) map[string]map[string]struct{} {
	// Don't create anything if we can never overlap.
	if len(entities) <= 1 {
		return nil
	}

	fields := make(map[string][]sreflect.ReflectStruct)
	for _, entity := range entities {
		// Group the entity fields associated with other entities with similar
		// names.
		for _, field := range entity.FieldNames() {
			fields[field] = append(fields[field], entity)
		}
	}

	// Read the group and identify the overlaps by the entity name, not via
	// the field (inverting the group).
	results := make(map[string]map[string]struct{})
	for fieldName, entities := range fields {
		// Ignore entities that aren't grouped.
		if len(entities) <= 1 {
			continue
		}

		for _, entity := range entities {
			if _, ok := results[entity.Name]; !ok {
				results[entity.Name] = make(map[string]struct{})
			}
			results[entity.Name][fieldName] = struct{}{}
		}
	}

	return results
}

type cachedStmt struct {
	stmt   string
	fields []recordBinding
}
type statementCache struct {
	mutex sync.Mutex
	cache map[string]cachedStmt
}

func newStatementCache() *statementCache {
	return &statementCache{
		cache: make(map[string]cachedStmt),
	}
}

func (c *statementCache) Get(stmt string) (cachedStmt, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	computed, ok := c.cache[stmt]
	return computed, ok
}

func (c *statementCache) Set(stmt string, computed cachedStmt) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.cache[stmt] = computed
}
