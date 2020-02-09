package sqliteserver

import (
	"regexp"
	"strings"

	"github.com/siddontang/go-mysql/mysql"
	"go.uber.org/zap"
)

// integer primary key autoincrement
func (z *zHandler) handleCommandCreateTable(query string) (*mysql.Result, error) {
	//logger.Debug("initial create table query", zap.String("query", query))
	query = strings.ReplaceAll(query, "`", "\"")
	r := regexp.MustCompile(`(tinyint|int|bigint)\s+(unsigned\s+)?AUTO_INCREMENT`)
	query = r.ReplaceAllString(query, "integer primary key autoincrement")
	// remove primary key constraint
	r = regexp.MustCompile(`,\s*PRIMARY KEY\s+\(".+"\)`)
	query = r.ReplaceAllString(query, "")
	//logger.Debug("final create table query", zap.String("query", query))
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

