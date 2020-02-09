package sqliteserver

import (
	"net"

	"github.com/siddontang/go-mysql/server"
	"go.uber.org/zap"
)

type ServerConfig struct {
	Network string
	Address string
	UserName string
	Password string
	DatabasePath string
}

type Server struct {
	listener net.Listener
	config   ServerConfig
	dbPool   DBPool
}

func NewServer(cfg ServerConfig) (*Server, error) {
	s := &Server{config:cfg, dbPool: newDBPool(cfg.DatabasePath)}
	return s, nil
}

func (s *Server)Start() error {
	l, err := net.Listen(s.config.Network, s.config.Address)
	if err != nil {
		logger.Error("failed to create net.Listen", zap.Error(err))
		return err
	}

	s.listener = l
	go func() {
		for {
			c, err := s.listener.Accept()
			if err != nil {
				logger.Error("failed to accept connection", zap.Error(err))
				return
			}

			go s.acceptNewConnection(c)
		}
	}()

	return nil
}

func (s *Server) Close() error {
	if s.listener == nil {
		return nil
	}

	err := s.listener.Close()
	s.listener = nil
	return err
}

func (s Server) acceptNewConnection(c net.Conn) {
	defer c.Close()

	h := NewHandler(s.dbPool)
	defer h.(*zHandler).Close()
	conn, err := server.NewConn(
		c,
		s.config.UserName,
		s.config.Password,
		h,
	)
	if err != nil {
		logger.Error("failed to start handler", zap.Error(err))
		return
	}

	for !conn.Closed() {
		err = conn.HandleCommand()
		if err != nil {
			logger.Error("failed to handle command", zap.Error(err))
			return
		}
	}

	logger.Debug("acceptNewConnection: exiting ...")
}
