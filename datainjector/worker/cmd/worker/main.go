package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/api"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/config"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/logging"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/metrics"
	roleruntime "github.com/twilight-labs/dataplatform/datainjector/worker/internal/role"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/status"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/tracing"
)

// 构建时注入的版本信息
var (
	Version   = "dev"
	BuildTime = "unknown"
)

func main() {
	cfgPath := flag.String("config", "./configs/base.yaml", "path to config file")
	roleRegistryPath := flag.String("roles", "", "path to role registry file (optional)")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	rootCtx := context.Background()

	logging.Init(logging.Config{
		ServiceName: cfg.Logging.ServiceName,
		Environment: cfg.Logging.Environment,
	})
	log.SetFlags(0)
	tracing.Init(tracing.Config{
		Enabled:           cfg.Tracing.Enabled,
		ServiceName:       cfg.Tracing.ServiceName,
		SampleRatio:       cfg.Tracing.SampleRatio,
		ForceSampleRunID:  cfg.Tracing.ForceSampleRunID,
		ForceSampleRoleID: cfg.Tracing.ForceSampleRoleID,
	})

	status.Init(cfg.StatusReporter)
	defer status.Close()

	// 启动metrics服务器
	var metricsServer *metrics.Server
	if cfg.Metrics.Enabled {
		metricsServer = metrics.NewServer(metrics.Config{
			Enabled: cfg.Metrics.Enabled,
			Port:    cfg.Metrics.Port,
			Path:    cfg.Metrics.Path,
		})
		if err := metricsServer.Start(); err != nil {
			logging.Error(rootCtx, logging.EventMetricsError, "failed to start metrics server", err, logging.Fields{
				"port": cfg.Metrics.Port,
			})
		} else {
			logging.Info(rootCtx, logging.EventMetricsStart, "metrics server started", logging.Fields{
				"port": cfg.Metrics.Port,
			})
		}
		// 设置构建信息
		metrics.SetBuildInfo(Version, runtime.Version(), BuildTime)
	}

	ctx, cancel := context.WithCancel(rootCtx)
	defer cancel()

	roleManager := roleruntime.NewManager(ctx, cfg.DataSources, cfg.RateLimit, cfg.RoleTemplates, cfg.Pipelines)
	defer roleManager.Shutdown()

	roles := cfg.Roles
	if *roleRegistryPath != "" {
		regRoles, err := config.LoadRoles(*roleRegistryPath)
		if err != nil {
			log.Fatalf("load role registry: %v", err)
		}
		roles = regRoles
	}

	if len(roles) > 0 {
		if err := roleManager.Apply(roles); err != nil {
			log.Fatalf("apply startup roles: %v", err)
		}
	} else {
		logging.Info(ctx, logging.EventRoleStartup, "no roles configured at startup, waiting for control API requests", nil)
	}

	// 优雅退出
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logging.Info(ctx, logging.EventWorkerShutdown, "received signal, shutting down", nil)
		cancel()
	}()

	var apiServer *api.Server
	if cfg.API.Enabled {
		apiServer = api.NewServer(cfg.API, roleManager, cfg.DataSources, cfg.RateLimit, cfg.RoleTemplates, cfg.Pipelines)
		apiServer.Start()
	}

	<-ctx.Done()

	if apiServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := apiServer.Stop(shutdownCtx); err != nil {
			logging.Error(ctx, logging.EventAPIShutdown, "control API shutdown error", err, nil)
		}
	}

	// 停止metrics服务器
	if metricsServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := metricsServer.Stop(shutdownCtx); err != nil {
			logging.Error(ctx, logging.EventMetricsError, "metrics server shutdown error", err, nil)
		}
	}

	// 给各 goroutine 留一点退出时间
	time.Sleep(300 * time.Millisecond)
}
