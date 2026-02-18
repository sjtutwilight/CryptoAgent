package handler

import (
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

// Handler 责任链节点接口
type Handler interface {
	Handle(msg *types.Message) ([]*types.Message, error)
}

// BackfillCommandAware 标记 handler 支持回补命令通道注入
// SetBackfillChannel 会在 Role 构建时由框架调用，用于下发缺失检测产生的补数据命令。
type BackfillCommandAware interface {
	SetBackfillChannel(ch chan<- types.BackfillCmd)
}

// BackfillTargetAware 支持按补数类型注入不同通道，实现 snapshot/range 隔离。
type BackfillTargetAware interface {
	SetBackfillTargets(defaultCh, snapshotCh, diffCh chan<- types.BackfillCmd)
}

// BackfillResultAware 接收 backfill 执行回执，用于结果驱动闭环状态迁移。
type BackfillResultAware interface {
	OnBackfillResult(result types.BackfillResult)
}

// SnapshotListener 接收快照应用完成的通知，返回可直接释放的增量消息。
type SnapshotListener interface {
	OnSnapshotApplied(lastSeq uint64) []*types.Message
}

// SnapshotListenerAware 表示 handler 能够感知快照完成事件。
type SnapshotListenerAware interface {
	SetSnapshotListener(listener SnapshotListener)
}

// NoopHandler 直接透传
type NoopHandler struct{}

func init() {
	Register("noop", func(cfg map[string]any) (Handler, error) {
		return &NoopHandler{}, nil
	})
}

func (n *NoopHandler) Handle(msg *types.Message) ([]*types.Message, error) {
	if msg == nil {
		return nil, nil
	}
	return []*types.Message{msg}, nil
}

// 为了向后兼容，保留这些函数作为 util 包的别名
var (
	getString = util.GetString
	getInt    = util.GetInt
)
