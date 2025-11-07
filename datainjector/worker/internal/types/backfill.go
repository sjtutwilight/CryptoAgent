package types

const (
	BackfillTransportWebSocket = "websocket"
	BackfillTransportHTTP      = "http"
)

const (
	BackfillTypeRange    = "range"    // 范围补数（区块链）
	BackfillTypeSnapshot = "snapshot" // 快照补数（订单簿）
)

type BackfillCmd struct {
	Type     string            // "range" 或 "snapshot"
	Start    int64             // 仅 range 类型使用
	End      int64             // 仅 range 类型使用
	Attempts []BackfillAttempt // 调度器生成的调用计划，按顺序尝试
}

type BackfillOption struct {
	Transport string
	RPCMethod string
	Params    map[string]any
}

// BackfillAttempt 描述一次调用尝试，由若干请求组成。
type BackfillAttempt struct {
	Name     string
	Requests []BackfillRequest
}

// BackfillRequest 描述一次对 caller 的调用参数。
type BackfillRequest struct {
	Transport string
	Args      map[string]any
}
