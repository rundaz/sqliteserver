package sqliteserver

import (
	"database/sql"

	"github.com/siddontang/go-mysql/mysql"
)

type statementHandler interface {
	SetStatement(stmt *sql.Stmt)
	Close() error
	Prepare(query string) (modifiedQuery string, params int, columns int, err error)
	Execute(query string, args []interface{}) (*mysql.Result, error)
}

func newStatementHandler(qt queryType) statementHandler {
	switch qt {
	case queryTypeInsert, queryTypeDelete, queryTypeUpdate:
		return &execStmt{}
	case queryTypeSelect:
		return &selectStmt{}
	case queryTypeShowColumns:
		return &showColumnsStmt{}
	case queryTypeShowTables:
		return &showtableStmt{}
	default:
		return &execStmt{}
	}
}
