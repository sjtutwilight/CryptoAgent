package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/config"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/role"
)

func main() {
	cfgPath := flag.String("config", "./configs/config.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	if len(cfg.Roles) == 0 {
		log.Fatalf("no roles in config")
	}

	// 仅启动配置中的所有角色（当前关注 localnode-block）
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 优雅退出
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Printf("received signal, shutting down...")
		cancel()
	}()

	var runners []*role.Role
	for _, rc := range cfg.Roles {
		r, err := role.Build(rc)
		if err != nil {
			log.Fatalf("build role %s: %v", rc.RoleID, err)
		}
		runners = append(runners, r)
	}

	// 并发启动
	errCh := make(chan error, len(runners))
	for _, r := range runners {
		go func(rn *role.Role) {
			log.Printf("starting role: %s", rn.ID)
			errCh <- rn.Start(ctx)
		}(r)
	}

	// 等待任一报错或退出
	select {
	case err := <-errCh:
		if err != nil {
			log.Printf("role exited with error: %v", err)
		}
	case <-ctx.Done():
	}

	// 给各 goroutine 留一点退出时间
	time.Sleep(300 * time.Millisecond)
}
