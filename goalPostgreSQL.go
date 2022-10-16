package goalPostgreSQL

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgtype"
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
// Please put your select arguments in inputParameters to prevent SQL Injection.
// Arguments should be referenced positionally from the SQL string as $1, $2, etc.
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
			return nil, fmt.Errorf(
				"PostgreSQL select query failed: syntax %q, query parameters %q, error %s",
				query, inputParameters, errorGetRows)
		}
		defer rows.Close()
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
		return nil, fmt.Errorf(
			"PostgreSQL select query failed: syntax %q, query parameters %q, error %s",
			query, inputParameters, errorGetRows)
	}
	defer rows.Close()
	// Restore the columns
	columns = append(columns, lastColumn)
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
