package sqliteserver

import (
	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

type showtableStmt struct {
	commonStmtHandler
}

func (s showtableStmt)Prepare(query string) (modifiedQuery string, params int, columns int, err error) {
	modifiedQuery = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
	params = 1
	columns = 1
	return
}

// Binary Protocol: i.e., using the above COM_STMT_PREPARE, COM_STMT_EXECUTE, COM_STMT_CLOSE
// the command and returns the results acquired Binary protocol, which is the manner commonly
// used in various applications developers.
func (s showtableStmt)Execute(query string, args []interface{}) (*mysql.Result, error) {
	tableName := uint8SliceTostring(args[0].([]uint8))
	rows, err := s.stmt.Query(tableName)
	if err != nil {
		logger.Error("error while query statement", zap.Error(err))
		return nil, err
	}
	return rowsToResult(rows, true)
}

