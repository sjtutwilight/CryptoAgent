package handler

import "github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"

// BackfillCommandAware 标记 handler 支持回补命令通道注入
// SetBackfillChannel 会在 Role 构建时由框架调用，用于下发缺失检测产生的补数据命令。
type BackfillCommandAware interface {
	SetBackfillChannel(ch chan<- types.BackfillCmd)
}
