package sqliteserver

import (
	"fmt"
	"regexp"

	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

type showColumnsStmt struct {
	commonStmtHandler
}

func (s showColumnsStmt)Prepare(query string) (modifiedQuery string, params int, columns int, err error) {
	params = 1
	columns = 1
	modifiedQuery = adjustShowColumns(query)
	return
}

func (s showColumnsStmt)Execute(query string, args []interface{}) (*mysql.Result, error) {
	columnName := uint8SliceTostring(args[0].([]uint8))
	var createSql string
	err := s.stmt.QueryRow().Scan(&createSql)
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

// adjustShowColumns will adjust original query (in MySQL mode) to Sqlite3 mode
// original query: SHOW COLUMNS FROM `{tableName}` FROM `{dbName}` WHERE Field = ?
// result query: SELECT sql FROM sqlite_master WHERE tbl_name = {tableName}
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

func uint8SliceTostring(bs []uint8) string {
	b := make([]byte, len(bs))
	for i, v := range bs {
		b[i] = byte(v)
	}
	return string(b)
}
