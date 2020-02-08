package sqliteserver

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/server"
	"go.uber.org/zap"
)

type zHandler struct {
	db          *sql.DB
	stmts       map[int]*sql.Stmt
	stmtCounter int
	mu          sync.Mutex
	tx          *sql.Tx
	pool        DBPool
	dbName      string
}

func NewHandler(pool DBPool) server.Handler {
	return &zHandler{
		stmts: make(map[int]*sql.Stmt),
		pool: pool,
	}
}

func (z *zHandler) UseDB(dbName string) error {
	logger.Debug("UseDB", zap.String("name", dbName))
	db, err := z.pool.UseDB(dbName)
	if err != nil {
		logger.Error("failed to open database",
			zap.String("name", dbName),
			zap.Error(err),
		)
		return err
	}
	z.db = db
	z.dbName = dbName
	return nil
}

func (z *zHandler)Close() {
	z.pool.Release(z.dbName)
}

// Text protocol: use COM_QUERY, and with PREPARE, EXECUTE, DEALLOCATE PREPARE
// using the text protocol to obtain a result, the efficiency is better on one of
// the non-program call for multiple scenes, such as manually performed in the MySQL client.
func (z *zHandler) HandleQuery(query string) (*mysql.Result, error) {
	logger.Debug("HandleQuery", zap.String("query", query))
	qt := getQueryType(query)
	switch qt {
	case queryTypeSelect:
		return z.handleSelect(query)
	case queryTypeStartTransaction:
		return z.handleBeginTransaction()
	case queryTypeCommit:
		return z.handleCommit()
	default:
		return nil, fmt.Errorf("not supported")
	}
}

func (z *zHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	logger.Debug("HandleFieldList",
		zap.String("table", table),
		zap.String("fields", fieldWildcard),
	)
	panic("implement me")
}

func (z *zHandler) HandleStmtPrepare(query string) (params int, columns int, context interface{}, err error) {
	logger.Debug("HandleStmtPrepare", zap.String("query", query))
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

	params = strings.Count(query, "?")
	switch getQueryType(query) {
	case queryTypeInsert, queryTypeDelete, queryTypeUpdate:
		columns = 0
	case queryTypeSelect:
		columns = 2
	}
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
	switch getQueryType(query) {
	case queryTypeInsert, queryTypeDelete, queryTypeUpdate:
		return z.doStmtExec(stmt, query, args)
	case queryTypeSelect:
		return z.doStmtQuery(stmt, query, args)
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

func (z *zHandler) HandleOtherCommand(cmd byte, data []byte) error {
	logger.Debug("HandleOtherCommand", zap.Any("cmd", cmd))
	return nil
}

func (z *zHandler) handleSelect(query string) (*mysql.Result, error) {
	rows, err := z.db.Query(query)
	if err != nil {
		return nil, err
	}

	return rowsToResult(rows, false)
}

func (z *zHandler) handleBeginTransaction() (*mysql.Result, error) {
	tx, err := z.db.Begin()
	if err != nil {
		logger.Error("error while beginning transaction", zap.Error(err))
		return nil, err
	}
	z.tx = tx
	return &mysql.Result{}, nil
}

func (z *zHandler) handleCommit() (*mysql.Result, error) {
	err := z.tx.Commit()
	if err != nil {
		return nil, err
	}

	z.tx = nil
	return &mysql.Result{}, nil
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

func rowsToResult(rows *sql.Rows, binary bool) (*mysql.Result, error) {
	// 2. Process result
	columns, _ := rows.Columns()
	scanArgs := make([]interface{}, len(columns))
	valueList := [][]interface{}{}

	affectedRows := uint64(0)
	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		// parse records
		err := rows.Scan(scanArgs...)
		if err != nil {
			logger.Error("column scan error", zap.Error(err))
			return nil, err
		}

		for i, _ := range values {
			//logger.Infow("scanned value", "value", values[i])
			if values[i] == nil {
				//logger.Info("adjust nil value")
				values[i] = []byte{}
			}
		}

		valueList = append(valueList, values)
		affectedRows ++
	}

	_ = rows.Close()
	result, err := mysql.BuildSimpleResultset(
		columns,
		valueList,
		binary,
	)
	if err != nil {
		logger.Error("build resultset error", zap.Error(err))
		return nil, err
	}

	//logger.Infow("return results", "affectedRows", affectedRows)
	return &mysql.Result{0, 0, affectedRows, result}, nil
}

type queryType int
const (
	queryTypeUnknown queryType = iota
	queryTypeSelect
	queryTypeInsert
	queryTypeUpdate
	queryTypeDelete
	queryTypeStartTransaction
	queryTypeCommit
)
func getQueryType(query string) queryType {
	prefix := strings.Split(strings.ToUpper(strings.TrimSpace(query)), " ")[0]
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
	}

	return queryTypeUnknown
}
