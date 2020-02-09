package sqliteserver

import "database/sql"

type commonStmtHandler struct {
	stmt *sql.Stmt
}

func (s *commonStmtHandler)SetStatement(stmt *sql.Stmt) {
	s.stmt = stmt
}

func (s *commonStmtHandler)Close() error {
	return s.stmt.Close()
}
