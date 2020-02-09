package sqliteserver

import (
	"database/sql"
	"strings"

	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

func (z *zHandler) HandleStmtPrepare(query string) (params int, columns int, context interface{}, err error) {
	logger.Debug("HandleStmtPrepare", zap.String("query", query))

	params = strings.Count(query, "?")

	qt := getQueryType(query)
	sh := newStatementHandler(qt)
	query, params, columns, err = sh.Prepare(query)

	var stmt *sql.Stmt
	if z.tx != nil {
		stmt, err = z.tx.Prepare(query)
	} else {
		stmt, err = z.db.Prepare(query)
	}
	if err != nil {
		logger.Error("error while prepare statement",
			zap.String("query", query),
			zap.Error(err),
		)
		return
	}

	sh.SetStatement(stmt)
	z.mu.Lock()
	z.stmtCounter ++
	z.stmts[z.stmtCounter] = sh
	z.mu.Unlock()

	context = z.stmtCounter
	return
}

func (z *zHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	logger.Debug("HandleStmtExecute",
		zap.Any("context", context),
		zap.String("query", query),
		zap.Any("args", args),
	)
	z.mu.Lock()
	stmt := z.stmts[context.(int)]
	z.mu.Unlock()

	return stmt.Execute(query, args)
}

func (z *zHandler) HandleStmtClose(context interface{}) error {
	logger.Debug("HandleStmtClose", zap.Any("context", context))
	z.mu.Lock()
	stmt := z.stmts[context.(int)]
	delete(z.stmts, context.(int))
	err := stmt.Close()
	z.mu.Unlock()
	return err
}
