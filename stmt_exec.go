package sqliteserver

import (
	"strings"

	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

type execStmt struct {
	commonStmtHandler
}

func (e execStmt)Prepare(query string) (modifiedQuery string, params int, columns int, err error) {
	modifiedQuery = query
	params = strings.Count(query, "?")
	return
}

func (e execStmt)Execute(query string, args []interface{}) (*mysql.Result, error) {
	rs, err := e.stmt.Exec(args...)
	if err != nil {
		logger.Error("error while exec statement", zap.Error(err))
		return nil, err
	}
	results := &mysql.Result{
		Status:       0,
		InsertId:     0,
		AffectedRows: 0,
		Resultset:    nil,
	}
	ii, err := rs.LastInsertId()
	num, err := rs.RowsAffected()
	results.InsertId = uint64(ii)
	results.AffectedRows = uint64(num)
	return results, err
}
