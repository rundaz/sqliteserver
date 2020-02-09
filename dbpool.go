package sqliteserver

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

type dbCounter struct {
	counter int64
	db *sql.DB
}
type dbPool struct {
	sync.Mutex

	dbs    map[string]*dbCounter
	dbPath string
}

type DBPool interface {
	UseDB(dbName string) (*sql.DB, error)
	Release(dbName string)
}

func newDBPool(dbPath string) DBPool {
	return &dbPool{
		dbPath: dbPath,
		dbs:   make(map[string]*dbCounter),
	}
}

func (p *dbPool)UseDB(dbName string) (*sql.DB, error) {
	p.Lock()
	defer p.Unlock()

	dbC, ok := p.dbs[dbName]
	if !ok {
		db, err := sql.Open("sqlite3", fmt.Sprintf("%s/%s", p.dbPath, dbName))
		if err != nil {
			logger.Error("cannot open database", zap.String("name", dbName), zap.Error(err))
			return nil, err
		}

		dbC = &dbCounter{
			counter: 0,
			db:      db,
		}
		p.dbs[dbName] = dbC
	}

	// increase use counter
	dbC.counter ++
	logger.Debug("use db", zap.String("name", dbName), zap.Int64("counter", dbC.counter))

	return dbC.db, nil
}

func (p *dbPool)Release(dbName string) {
	p.Lock()
	defer p.Unlock()
	dbC, ok := p.dbs[dbName]
	if !ok {
		logger.Error("database is not existed", zap.String("name", dbName))
		return
	}
	logger.Debug("release db - decreasing used counter", zap.String("name", dbName), zap.Int64("counter", dbC.counter))
	dbC.counter --
	if dbC.counter <= 0 {
		logger.Debug("delete db", zap.String("name", dbName))
		dbC.db.Close()
		delete(p.dbs, dbName)
	}
}
