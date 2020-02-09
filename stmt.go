package sqliteserver

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

func (z *zHandler) HandleStmtPrepare(query string) (params int, columns int, context interface{}, err error) {
	logger.Debug("HandleStmtPrepare", zap.String("query", query))

	params = strings.Count(query, "?")

	qt := getQueryType(query)
	switch qt {
	case queryTypeShowTables:
		// translate from mysql to sqlite3
		query = "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?"
		params = 1
	case queryTypeShowColumns:
		// original query: SHOW COLUMNS FROM `{tableName}` FROM `{dbName}` WHERE Field = ?
		query = adjustShowColumns(query)
		params = 1
	}

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

	z.mu.Lock()
	z.stmtCounter ++
	z.stmts[z.stmtCounter] = stmt
	z.mu.Unlock()

	switch qt {
	case queryTypeInsert, queryTypeDelete, queryTypeUpdate:
		columns = 0
	case queryTypeSelect:
		columns = 2
	}
	context = z.stmtCounter
	return
}

// adjustShowColumns will adjust original query (in MySQL mode) to Sqlite3 mode
// original query: SHOW COLUMNS FROM `{tableName}` FROM `{dbName}` WHERE Field = ?
// result query: SELECT count(*) FROM sqlite_master WHERE tbl_name = {tableName} AND (sql LIKE '%%\"%v\" %%' OR sql LIKE '%%%v %%');
func adjustShowColumns(query string) string {
	r, err := regexp.Compile("SHOW\\s+COLUMNS\\s+FROM\\s+[`]?(?P<TableName>\\w+)[`]?")
	if err != nil {
		logger.Warn("failed to compile regex expression for capturing table name", zap.Error(err))
		return query
	}
	m := r.FindStringSubmatch(query)
	if len(m) == 0 {
		logger.Info("input query does not match expected SHOW COLUMNS expression", zap.String("query", query))
		return query
	}

	tableName := m[1]
	query = fmt.Sprintf(`SELECT sql FROM sqlite_master WHERE tbl_name = "%s"`, tableName)
	return query
}

func (z *zHandler)handleShowColumns(stmt *sql.Stmt, args []interface{}) (*mysql.Result, error) {
	columnName := uint8SliceTostring(args[0].([]uint8))
	var createSql string
	err := stmt.QueryRow().Scan(&createSql)
	if err != nil {
		logger.Warn("error while query show columns statement", zap.Error(err))
	}
	r, err := regexp.Compile(fmt.Sprintf("(\"%s\" )|('%s' )|(`%s` )", columnName, columnName, columnName))
	if err != nil {
		logger.Warn("cannot build regex to capture column name", zap.Error(err))
		return &mysql.Result{}, nil
	}
	if len(r.FindStringSubmatch(createSql)) == 0 {
		logger.Debug(fmt.Sprintf("column `%s` does not exist", columnName))
		return &mysql.Result{}, nil
	}
	logger.Debug(fmt.Sprintf("column `%s` exists", columnName))
	result, err := mysql.BuildSimpleBinaryResultset([]string{"name"}, [][]interface{}{{columnName}})
	if err != nil {
		logger.Error("build resultset error", zap.Error(err))
		return nil, err
	}
	return &mysql.Result{AffectedRows:1, Resultset: result}, nil
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

	switch getQueryType(query) {
	case queryTypeInsert, queryTypeDelete, queryTypeUpdate:
		return z.doStmtExec(stmt, query, args)
	case queryTypeSelect:
		return z.doStmtQuery(stmt, query, args)
	case queryTypeShowColumns:
		return z.handleShowColumns(stmt, args)
	default:
		return z.doStmtQuery(stmt, query, args)
	}
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


func (z *zHandler) doStmtExec(stmt *sql.Stmt, query string, args []interface{}) (*mysql.Result, error) {
	rs, err := stmt.Exec(args...)
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

// Binary Protocol: i.e., using the above COM_STMT_PREPARE, COM_STMT_EXECUTE, COM_STMT_CLOSE
// the command and returns the results acquired Binary protocol, which is the manner commonly
// used in various applications developers.
func (z *zHandler) doStmtQuery(stmt *sql.Stmt, query string, args []interface{}) (*mysql.Result, error) {
	rows, err := stmt.Query(args...)
	if err != nil {
		logger.Error("error while query statement", zap.Error(err))
		return nil, err
	}
	return rowsToResult(rows, true)
}

func uint8SliceTostring(bs []uint8) string {
	b := make([]byte, len(bs))
	for i, v := range bs {
		b[i] = byte(v)
	}
	return string(b)
}
