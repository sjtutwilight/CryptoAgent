// Package metrics 提供metrics HTTP server
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/logging"
)

// Server Prometheus metrics HTTP服务器
type Server struct {
	server *http.Server
	port   int
}

// Config metrics服务器配置
type Config struct {
	Enabled bool   `yaml:"enabled"`
	Port    int    `yaml:"port"` // 默认9100
	Path    string `yaml:"path"` // 默认/metrics
}

// DefaultConfig 返回默认配置
func DefaultConfig() Config {
	return Config{
		Enabled: true,
		Port:    9100,
		Path:    "/metrics",
	}
}

// NewServer 创建metrics服务器
func NewServer(cfg Config) *Server {
	if cfg.Port == 0 {
		cfg.Port = 9100
	}
	if cfg.Path == "" {
		cfg.Path = "/metrics"
	}

	mux := http.NewServeMux()
	mux.Handle(cfg.Path, Handler())

	// 健康检查端点
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// 就绪检查端点
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Ready"))
	})

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	return &Server{
		server: server,
		port:   cfg.Port,
	}
}

// Start 启动metrics服务器（非阻塞）
func (s *Server) Start() error {
	logging.Info(context.Background(), logging.EventMetricsStarting, "starting metrics server", logging.Fields{
		"port": s.port,
	})
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logging.Warn(context.Background(), logging.EventMetricsError, "metrics server error", logging.Fields{
				"error": err.Error(),
			})
		}
	}()
	return nil
}

// Stop 优雅停止服务器
func (s *Server) Stop(ctx context.Context) error {
	logging.Info(context.Background(), logging.EventMetricsStop, "stopping metrics server", logging.Fields{
		"port": s.port,
	})
	return s.server.Shutdown(ctx)
}

// Port 返回服务器端口
func (s *Server) Port() int {
	return s.port
}







