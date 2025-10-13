package caller

import (
	"context"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

// BlockFetcher 抽象补数执行能力，由 Role 调用。
type BlockFetcher interface {
	FetchBlocks(ctx context.Context, start, end int64, rpcMethod string, options map[string]any) ([]*types.Message, error)
	TransportName() string
}

// BackfillProvider 允许 caller 暴露额外的补数执行器（例如 WS Caller 附带 HTTP 回补）。
type BackfillProvider interface {
	BackfillExecutors() []BlockFetcher
}
