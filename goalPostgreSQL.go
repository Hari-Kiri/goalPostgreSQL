package goalPostgreSQL

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Init postgres connection.
func PgPoolConnect(databaseURL string) (*pgxpool.Pool, pgtype.Text, error) {
	var pgVersionNil pgtype.Text
	// urlExample := "postgres://username:password@localhost:5432/database_name"
	connectionPool, errorConnection := pgxpool.New(context.Background(), databaseURL)
	if errorConnection != nil {
		// log.Fatalln("Unable to connect to database: ", errorConnection)
		return nil, pgVersionNil, fmt.Errorf("unable to connect to database: %q", errorConnection)
	}
	// Get postgres version
	var postgreVersion pgtype.Text
	errorSelectVersion := connectionPool.QueryRow(context.Background(), "SELECT VERSION()").Scan(&postgreVersion)
	if errorSelectVersion != nil {
		return nil, pgVersionNil, fmt.Errorf("unable to log PostgreSQL version: %q", errorSelectVersion)
	}
	return connectionPool, postgreVersion, nil
}

// Close PostgreSQL connection
func PgClose(connectionPool *pgxpool.Pool) {
	defer connectionPool.Close()
}

// PostgreSQL select query for multiple rows of data.
// Please put your select query arguments in inputParameters to prevent SQL Injection.
// Arguments should be referenced positionally from the SQL string as $1, $2, etc.
//
// Function use example:
// goalPostgreSQL.PgSelect(connectionPool, []string{"id"}, "database.public.users", "WHERE username = $1 AND password = $2", username, password)
func PgSelect(connectionPool *pgxpool.Pool, columns []string, table string,
	condition string, inputParameters ...any) ([]map[string]interface{}, error) {
	/* Column 0 (error!!!) */
	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns selected: %q", columns)
	}
	/* Single column query */
	if len(columns) == 1 {
		// Execute query
		query := "SELECT " + columns[0] + " FROM " + table + " " + condition
		rows, errorGetRows := connectionPool.Query(context.Background(), query, inputParameters...)
		if errorGetRows != nil {
			return nil, fmt.Errorf("PostgreSQL select query failed: syntax %q, query parameters %q, error %s",
				query, inputParameters, errorGetRows)
		}
		defer rows.Close()
		// Column name casting
		for index, column := range columns {
			regexp := regexp.MustCompile(`^(.*? as \b)|(.*? AS \b).*?`)
			column = regexp.ReplaceAllString(column, "")
			columns[index] = column
		}
		// Make map string interface array variable to hold this function result
		result := make([]map[string]interface{}, 0)
		// Iterate query result
		for rows.Next() {
			// Make temporary interface to store PostgreSQL query result value
			values := make([]interface{}, len(columns))
			// Every values returned from PostgreSQL query assign to a string pointer
			// and all the memory adresses store in temporary interface for further process
			for index := range columns {
				var stringPointer any
				values[index] = &stringPointer
			}
			// Scan rows from PostgreSQL query result
			errorGetRows = rows.Scan(values...)
			if errorGetRows != nil {
				return nil, fmt.Errorf("failed to scan rows: %s", errorGetRows)
			}
			// Make map string interface variable to store temporary interface values
			mapStringInterface := make(map[string]interface{})
			// Read every pointer value from temporary interface then store all the data
			// to map string interface variable
			for index, value := range values {
				pointer := reflect.ValueOf(value)
				queryResult := pointer.Interface()
				if pointer.Kind() == reflect.Ptr {
					queryResult = pointer.Elem().Interface()
				}
				mapStringInterface[columns[index]] = queryResult
			}
			// Store all data from map string interface variable
			// to map string interface array variable
			result = append(result, mapStringInterface)
		}
		return result, nil
	}
	/* Multiple columns query */
	// Extract columns parameter to syntax string
	var columnString strings.Builder
	// Get last column from columns parameter
	lastColumn := columns[len(columns)-1]
	// Delete last column from columns parameter
	columns = columns[:len(columns)-1]
	// Extract columns
	for _, column := range columns {
		columnString.WriteString(column + ", ")
	}
	// Execute query
	query := "SELECT " + columnString.String() + lastColumn + " FROM " + table + " " + condition
	rows, errorGetRows := connectionPool.Query(context.Background(), query, inputParameters...)
	if errorGetRows != nil {
		return nil, fmt.Errorf("PostgreSQL select query failed: syntax %q, query parameters %q, error %s",
			query, inputParameters, errorGetRows)
	}
	defer rows.Close()
	// Restore the columns
	columns = append(columns, lastColumn)
	// Columns name casting
	for index, column := range columns {
		regexp := regexp.MustCompile(`^(.*? as \b)|(.*? AS \b).*?`)
		column = regexp.ReplaceAllString(column, "")
		columns[index] = column
	}
	// Make map string interface array variable to hold this function result
	result := make([]map[string]interface{}, 0)
	// Iterate query result
	for rows.Next() {
		// Make temporary interface to store PostgreSQL query result value
		values := make([]interface{}, len(columns))
		// Every values returned from PostgreSQL query assign to a string pointer
		// and all the memory adresses store in temporary interface for further process
		for index := range columns {
			var stringPointer any
			values[index] = &stringPointer
		}
		// Scan rows from PostgreSQL query result
		errorGetRows = rows.Scan(values...)
		if errorGetRows != nil {
			return nil, fmt.Errorf("failed to scan rows: %s", errorGetRows)
		}
		// Make map string interface variable to store temporary interface values
		mapStringInterface := make(map[string]interface{})
		// Read every pointer value from temporary interface then store all the data
		// to map string interface variable
		for index, value := range values {
			pointer := reflect.ValueOf(value)
			queryResult := pointer.Interface()
			if pointer.Kind() == reflect.Ptr {
				queryResult = pointer.Elem().Interface()
			}
			mapStringInterface[columns[index]] = queryResult
		}
		// Store all data from map string interface variable
		// to map string interface array variable
		result = append(result, mapStringInterface)
	}
	return result, nil
}

// Update PostgreSQL table. On success update this method will return how many rows updated.
// Please put your update query arguments in inputParameters to prevent SQL Injection.
// Arguments should be referenced positionally from the SQL string as $1, $2 and etc.
// Inserted arguments reference in condition parameter must be count after the last columns.
//
// Example:
// There is 7 columns will be update, so the arguments reference in condition parameter should start from $8.
//
// Function use example:
// goalPostgreSQL.PgUpdate(connectionPool, "database.public.users", []string{column1, column2, column3}, "WHERE id = $4", valueColumn1, valueColumn2, valueColumn3)
//
// Function use with PostgreSQL append function:
// goalPostgreSQL.PgUpdate(connectionPool, "database.public.users", []string{column1.append, column2.append, column3.append}, "WHERE id = $4", valueColumn1, valueColumn2, valueColumn3)
func PgUpdate(connectionPool *pgxpool.Pool, table string, columns []string, condition string, inputParameters ...any) (int64, error) {
	/* Column 0 (error!!!) */
	if len(columns) == 0 {
		return 0, fmt.Errorf("no column selected for update: %q", columns)
	}
	/* Single column update query */
	if len(columns) == 1 {
		// PostgreSQL update query
		query := "UPDATE " + table + " SET " + columns[0] + " = $1 " + condition
		executeQuery, errorExecutingQuery := connectionPool.Exec(context.Background(), query, inputParameters...)
		if errorExecutingQuery != nil {
			return 0, fmt.Errorf("PostgreSQL update query failed: syntax %q, query parameters %q, error %s",
				query, inputParameters, errorExecutingQuery)
		}
		// Return the total of rows updated
		return executeQuery.RowsAffected(), nil
	}
	/* Multi columns update query */
	// Create update value parameter placeholders
	var columnPlaceholders strings.Builder
	// Get last column from columns parameter and add last argument position reference
	var lastColumn string
	if strings.Contains(columns[len(columns)-1], "append") {
		columnName := strings.Split(columns[len(columns)-1], ".")
		lastColumn = columnName[0] + " = array_append(" + columnName[0] + ", $" + strconv.Itoa(len(columns)) + ")"
	}
	if !strings.Contains(columns[len(columns)-1], "append") {
		lastColumn = columns[len(columns)-1] + " = $" + strconv.Itoa(len(columns))
	}
	// Delete last column from columns parameter
	columns = columns[:len(columns)-1]
	// for index, column := range columns {
	// 	columnPlaceholders.WriteString(column + " = $" + strconv.Itoa(index+1) + ", ")
	// }
	for index := 0; index < len(columns); index++ {
		if strings.Contains(columns[index], "append") {
			columnName := strings.Split(columns[index], ".")
			columnPlaceholders.WriteString(columnName[0] + " = array_append(" + columnName[0] + ", $" + strconv.Itoa(index+1) + "), ")
		}
		if !strings.Contains(columns[index], "append") {
			columnPlaceholders.WriteString(columns[index] + " = $" + strconv.Itoa(index+1) + ", ")
		}
	}
	// PostgreSQL update query
	query := "UPDATE " + table + " SET " + columnPlaceholders.String() + lastColumn + " " + condition
	executeQuery, errorExecutingQuery := connectionPool.Exec(context.Background(), query, inputParameters...)
	if errorExecutingQuery != nil {
		return 0, fmt.Errorf("PostgreSQL update query failed: syntax %q, query parameters %q, error %s",
			query, inputParameters, errorExecutingQuery)
	}
	// Return the total of rows updated
	return executeQuery.RowsAffected(), nil
}

// Insert data to PostgreSQL table. On success insert this method will return how many rows inserted.
// Please put your update query arguments in inputParameters to prevent SQL Injection.
// Arguments should be referenced positionally from the SQL string as $1, $2 and etc.
//
// Function use example:
// goalPostgreSQL.PgInsert(connectionPool, "database.public.users", []string{"my_column1", "my_column2", "my_column3"}, "value of column1", "value of column2", "value of column3")
// func PgInsert(connectionPool *pgxpool.Pool, table string, columns []string, inputParameters ...any) (int64, error) {
// 	if len(columns) == 0 {
// 		return 0, fmt.Errorf("no column: %q", columns)
// 	}
// 	// Single column insert
// 	if len(columns) == 1 {
// 		column := columns[0]
// 		// Sql insert query
// 		query := "INSERT INTO " + table + " (" + column + ") VALUES ($1)"
// 		executeQuery, errorExecutingQuery := connectionPool.Exec(context.Background(), query, inputParameters...)
// 		if errorExecutingQuery != nil {
// 			return 0, fmt.Errorf("PostgreSQL insert query failed: syntax %q, query parameters %q, error %s",
// 				query, inputParameters, errorExecutingQuery)
// 		}
// 		// Return the total of rows updated
// 		return executeQuery.RowsAffected(), nil
// 	}
// 	// Create value parameter placeholders
// 	var valuePlaceholders strings.Builder
// 	valuePlaceholders.WriteString("$1")
// 	for index := 1; index < len(inputParameters); index++ {
// 		valuePlaceholders.WriteString(", $" + strconv.Itoa(index+1))
// 	}
// 	// Extract columns parameter to syntax string
// 	var columnString strings.Builder
// 	// Get last column from columns parameter
// 	lastColumn := columns[len(columns)-1]
// 	// Delete last column from columns parameter
// 	columns = columns[:len(columns)-1]
// 	// Extract columns
// 	for _, column := range columns {
// 		columnString.WriteString(column + ", ")
// 	}
// 	// Sql insert query
// 	query := "INSERT INTO " + table + " (" + columnString.String() + lastColumn + ") VALUES (" +
// 		valuePlaceholders.String() + ")"
// 	executeQuery, errorExecutingQuery := connectionPool.Exec(context.Background(), query, inputParameters...)
// 	if errorExecutingQuery != nil {
// 		return 0, fmt.Errorf("PostgreSQL insert query failed: syntax %q, query parameters %q, error %s",
// 			query, inputParameters, errorExecutingQuery)
// 	}
// 	// Return the total of rows updated
// 	return executeQuery.RowsAffected(), nil
// }

// Insert data to PostgreSQL table. On success insert this method will return data inserted primary key.
// Please put your insert query arguments in inputParameters to prevent SQL Injection.
// Arguments should be referenced positionally from the SQL string as $1, $2 and etc.
//
// Function use example:
// goalPostgreSQL.PgInsertOne(connectionPool, "database.public.users", []string{"my_column1", "my_column2", "my_column3"}, "my_id_column_primary_key", "value of column1", "value of column2", "value of column3")
func PgInsert(connectionPool *pgxpool.Pool, table string, columns []string, columnPrimaryKey string, inputParameters ...any) (int64, error) {
	if len(columns) == 0 {
		return 0, fmt.Errorf("no column selected for insert data: %q", columns)
	}
	// Single column insert
	if len(columns) == 1 {
		column := columns[0]
		// Sql insert query
		query := "INSERT INTO " + table + " (" + column + ") VALUES ($1) RETURNING " + columnPrimaryKey
		id := 0
		errorExecutingQuery := connectionPool.QueryRow(context.Background(), query, inputParameters...).Scan(&id)
		if errorExecutingQuery != nil {
			return 0, fmt.Errorf("PostgreSQL insert query failed: syntax %q, query parameters %q, error %s",
				query, inputParameters, errorExecutingQuery)
		}
		// Return the id of row inserted
		return int64(id), nil
	}
	// Create value parameter placeholders
	var valuePlaceholders strings.Builder
	valuePlaceholders.WriteString("$1")
	for index := 1; index < len(inputParameters); index++ {
		valuePlaceholders.WriteString(", $" + strconv.Itoa(index+1))
	}
	// Extract columns parameter to syntax string
	var columnString strings.Builder
	// Get last column from columns parameter
	lastColumn := columns[len(columns)-1]
	// Delete last column from columns parameter
	columns = columns[:len(columns)-1]
	// Extract columns
	for _, column := range columns {
		columnString.WriteString(column + ", ")
	}
	// Sql insert query
	query := "INSERT INTO " + table + " (" + columnString.String() + lastColumn + ") VALUES (" +
		valuePlaceholders.String() + ") RETURNING " + columnPrimaryKey
	id := 0
	errorExecutingQuery := connectionPool.QueryRow(context.Background(), query, inputParameters...).Scan(&id)
	if errorExecutingQuery != nil {
		return 0, fmt.Errorf("PostgreSQL insert query failed: syntax %q, query parameters %q, error %s",
			query, inputParameters, errorExecutingQuery)
	}
	// Return the id of row inserted
	return int64(id), nil
}

// Delete data to PostgreSQL table. On success delete this method will return how many rows data deleted.
// Please put your insert query arguments in inputParameters to prevent SQL Injection.
// Arguments should be referenced positionally from the SQL string as $1, $2 and etc.
//
// Function use example:
// pgDelete(connectionPool, "database.public.users", []string{"my_column1", "my_column2", "my_column3"}, "value of column1", "value of column2", "value of column3")
func PgDelete(connectionPool *pgxpool.Pool, table string, columns []string, inputParameters ...any) (int64, error) {
	if len(columns) == 0 {
		return 0, fmt.Errorf("no column selected for delete data: %q", columns)
	}
	/* Single column delete query */
	if len(columns) == 1 {
		// PostgreSQL delete query
		query := "DELETE FROM " + table + " WHERE " + columns[0] + " = $1"
		executeQuery, errorExecutingQuery := connectionPool.Exec(context.Background(), query, inputParameters...)
		if errorExecutingQuery != nil {
			return 0, fmt.Errorf("PostgreSQL delete query failed: syntax %q, query parameters %q, error %s",
				query, inputParameters, errorExecutingQuery)
		}
		// Return the total of rows deleted
		return executeQuery.RowsAffected(), nil
	}
	/* Multi columns delete query */
	// Create delete value parameter placeholders
	var columnPlaceholders strings.Builder
	// Get last column from columns parameter and add last argument position reference
	lastColumn := columns[len(columns)-1] + " = $" + strconv.Itoa(len(columns))
	// Delete last column from columns parameter
	columns = columns[:len(columns)-1]
	for index, column := range columns {
		columnPlaceholders.WriteString(column + " = $" + strconv.Itoa(index+1) + " AND ")
	}
	// PostgreSQL delete query
	query := "DELETE FROM " + table + " WHERE " + columnPlaceholders.String() + lastColumn
	executeQuery, errorExecutingQuery := connectionPool.Exec(context.Background(), query, inputParameters...)
	if errorExecutingQuery != nil {
		return 0, fmt.Errorf("PostgreSQL delete query failed: syntax %q, query parameters %q, error %s",
			query, inputParameters, errorExecutingQuery)
	}
	// Return the total of rows deleted
	return executeQuery.RowsAffected(), nil
}
