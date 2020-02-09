package sqliteserver

import (
	"strings"

	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

type selectStmt struct {
	commonStmtHandler
}

func (s selectStmt)Prepare(query string) (modifiedQuery string, params int, columns int, err error) {
	modifiedQuery = query
	params = strings.Count(query, "?")
	columns = 2
	return
}

// Binary Protocol: i.e., using the above COM_STMT_PREPARE, COM_STMT_EXECUTE, COM_STMT_CLOSE
// the command and returns the results acquired Binary protocol, which is the manner commonly
// used in various applications developers.
func (s selectStmt)Execute(query string, args []interface{}) (*mysql.Result, error) {
	rows, err := s.stmt.Query(args...)
	if err != nil {
		logger.Error("error while query statement", zap.Error(err))
		return nil, err
	}
	return rowsToResult(rows, true)
}
