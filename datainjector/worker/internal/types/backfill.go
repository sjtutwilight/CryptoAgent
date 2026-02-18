package types

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	BackfillTransportWebSocket = "websocket"
	BackfillTransportHTTP      = "http"
)

const (
	BackfillTypeRange    = "range"    // 范围补数（区块链）
	BackfillTypeSnapshot = "snapshot" // 快照补数（订单簿）
)

var (
	ErrBackfillNoTarget       = errors.New("backfill no target")
	ErrBackfillQueueFull      = errors.New("backfill queue full")
	ErrBackfillEnqueueTimeout = errors.New("backfill enqueue timeout")
)

const (
	BackfillResultSuccess = "success"
	BackfillResultFail    = "fail"
	BackfillResultTimeout = "timeout"
)

func BackfillErrorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrBackfillNoTarget):
		return "no_target"
	case errors.Is(err, ErrBackfillQueueFull):
		return "queue_full"
	case errors.Is(err, ErrBackfillEnqueueTimeout):
		return "enqueue_timeout"
	default:
		return "unknown"
	}
}

func NormalizeBackfillErrorClass(class string) string {
	switch strings.ToLower(strings.TrimSpace(class)) {
	case "":
		return ""
	case "no_target":
		return "no_target"
	case "queue_full":
		return "queue_full"
	case "enqueue_timeout":
		return "enqueue_timeout"
	case "timeout":
		return "timeout"
	default:
		return "unknown"
	}
}

func BackfillSessionKey(roleID, streamKey, kind string) string {
	roleID = strings.TrimSpace(roleID)
	if roleID == "" {
		roleID = "unknown"
	}
	streamKey = strings.TrimSpace(streamKey)
	if streamKey == "" {
		streamKey = "default"
	}
	kind = strings.TrimSpace(kind)
	if kind == "" {
		kind = "unknown"
	}
	return fmt.Sprintf("%s|%s|%s", roleID, streamKey, kind)
}

type BackfillCmd struct {
	CmdID          string            `json:"cmd_id,omitempty"`          // 本次调度命令唯一标识
	SessionID      string            `json:"session_id,omitempty"`      // 单飞会话标识
	Key            string            `json:"key,omitempty"`             // role+stream+type 幂等键
	Attempt        int               `json:"attempt,omitempty"`         // 当前命令尝试序号，从 1 开始
	Type           string            `json:"type"`                      // "range" 或 "snapshot"
	RoleID         string            `json:"role_id"`                   // 触发补数的 role_id
	StreamKey      string            `json:"stream_key"`                // 数据流标识
	Start          int64             `json:"start"`                     // 仅 range 类型使用
	End            int64             `json:"end"`                       // 仅 range 类型使用
	SnapshotSource string            `json:"snapshot_source,omitempty"` // snapshot 来源，如 backfill/periodic
	SnapshotReason string            `json:"snapshot_reason,omitempty"` // snapshot 原因，如 gap/timeout/periodic
	Attempts       []BackfillAttempt `json:"attempts,omitempty"`        // 调度器生成的调用计划，按顺序尝试
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

type BackfillResult struct {
	CmdID       string    `json:"cmd_id,omitempty"`
	SessionID   string    `json:"session_id,omitempty"`
	Key         string    `json:"key,omitempty"`
	RoleID      string    `json:"role_id,omitempty"`
	StreamKey   string    `json:"stream_key,omitempty"`
	Type        string    `json:"type,omitempty"`
	Attempt     int       `json:"attempt,omitempty"`
	Status      string    `json:"status,omitempty"`      // success/fail/timeout
	ErrorClass  string    `json:"error_class,omitempty"` // no_target/queue_full/enqueue_timeout/timeout/unknown
	SnapshotSeq uint64    `json:"snapshot_seq,omitempty"`
	FinishedAt  time.Time `json:"finished_at,omitempty"`
}

func (cmd *BackfillCmd) EnsureDefaults(now time.Time) {
	if cmd == nil {
		return
	}
	if cmd.Attempt <= 0 {
		cmd.Attempt = 1
	}
	if cmd.Key == "" {
		cmd.Key = BackfillSessionKey(cmd.RoleID, cmd.StreamKey, cmd.Type)
	}
	if cmd.SessionID == "" {
		cmd.SessionID = fmt.Sprintf("%s#%d", cmd.Key, now.UnixNano())
	}
	if cmd.CmdID == "" {
		cmd.CmdID = fmt.Sprintf("%s@%d", cmd.SessionID, now.UnixNano())
	}
}

func (result *BackfillResult) EnsureDefaultsFromCmd(cmd BackfillCmd, now time.Time) {
	if result == nil {
		return
	}
	cmd.EnsureDefaults(now)
	if result.CmdID == "" {
		result.CmdID = cmd.CmdID
	}
	if result.SessionID == "" {
		result.SessionID = cmd.SessionID
	}
	if result.Key == "" {
		result.Key = cmd.Key
	}
	if result.RoleID == "" {
		result.RoleID = cmd.RoleID
	}
	if result.StreamKey == "" {
		result.StreamKey = cmd.StreamKey
	}
	if result.Type == "" {
		result.Type = cmd.Type
	}
	if result.Attempt <= 0 {
		result.Attempt = cmd.Attempt
	}
	if result.ErrorClass != "" {
		result.ErrorClass = NormalizeBackfillErrorClass(result.ErrorClass)
	}
	if result.FinishedAt.IsZero() {
		result.FinishedAt = now
	}
}
