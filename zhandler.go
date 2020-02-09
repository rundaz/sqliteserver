package sqliteserver

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/server"
	"go.uber.org/zap"
)

type zHandler struct {
	db          *sql.DB
	stmts       map[int]statementHandler
	stmtCounter int
	mu          sync.Mutex
	tx          *sql.Tx
	pool        DBPool
	dbName      string
}

func NewHandler(pool DBPool) server.Handler {
	return &zHandler{
		stmts: make(map[int]statementHandler),
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
	case queryTypeCreateTable:
		return z.handleExecQuery(query)
	case queryTypeSelectDatabase:
		return handleSelectDatabase(z.dbName)
	case queryTypeAlterTable:
		return z.handleAlterTable(query)
	default:
		return nil, fmt.Errorf("not supported")
	}
}

func handleSelectDatabase(dbName string) (*mysql.Result, error) {
	result, err := mysql.BuildSimpleTextResultset([]string{"name"}, [][]interface{}{{dbName}})
	if err != nil {
		logger.Error("build resultset error", zap.Error(err))
		return nil, err
	}

	return &mysql.Result{0, 0, 0, result}, nil
}

func (z *zHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	logger.Debug("HandleFieldList",
		zap.String("table", table),
		zap.String("fields", fieldWildcard),
	)
	panic("implement me")
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

func (z *zHandler) handleExecQuery(query string) (*mysql.Result, error) {
	rs, err := z.db.Exec(query)
	if err != nil {
		logger.Error("error while exec statement", zap.Error(err))
		return nil, err
	}
	ii, err := rs.LastInsertId()
	num, err := rs.RowsAffected()
	return &mysql.Result{
		Status:       0,
		InsertId:     uint64(ii),
		AffectedRows: uint64(num),
		Resultset:    nil,
	}, err
}

func (z *zHandler) handleAlterTable(query string) (*mysql.Result, error) {
	return z.handleExecQuery(query)
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

	logger.Debug("return results", zap.Uint64("affectedRows", affectedRows))
	return &mysql.Result{0, 0, affectedRows, result}, nil
}
