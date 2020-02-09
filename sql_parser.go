package sqliteserver

import "strings"

type queryType int
const (
	queryTypeUnknown queryType = iota
	queryTypeSelect
	queryTypeInsert
	queryTypeUpdate
	queryTypeDelete
	queryTypeStartTransaction
	queryTypeCommit
	queryTypeCreateTable
	queryTypeSelectDatabase
	queryTypeShowTables
	queryTypeShowColumns
	queryTypeAlterTable
)

func getQueryType(query string) queryType {
	query = strings.ToUpper(strings.TrimSpace(query))
	if strings.HasPrefix(query, "SELECT DATABASE()") {
		return queryTypeSelectDatabase
	}

	if strings.HasPrefix(query, "CREATE TABLE") {
		return queryTypeCreateTable
	}

	if strings.HasPrefix(query, "SHOW TABLES") {
		return queryTypeShowTables
	}

	if strings.HasPrefix(query, "SHOW COLUMNS") {
		return queryTypeShowColumns
	}

	if strings.HasPrefix(query, "START TRANSACTION") {
		return queryTypeStartTransaction
	}

	if strings.HasPrefix(query, "ALTER TABLE") {
		return queryTypeAlterTable
	}

	prefix := strings.Split(query, " ")[0]
	switch prefix {
	case "INSERT":
		return queryTypeInsert
	case "DELETE":
		return queryTypeDelete
	case "UPDATE":
		return queryTypeUpdate
	case "START":
		return queryTypeStartTransaction
	case "COMMIT":
		return queryTypeCommit
	case "SELECT":
		return queryTypeSelect
	case "CREATE":
		return queryTypeCreateTable
	}

	return queryTypeUnknown
}

