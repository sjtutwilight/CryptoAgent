package caller

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/logging"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/protocol"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/util"
)

type WebSocketCall struct {
	chainID              string
	roleID               string
	subscribeTopic       string
	subscribePayload     interface{}
	subscribeReq         protocol.JSONRPCRequest
	subscribeMethod      string
	subscribeJSONRPC     string
	subscribeReqIDs      map[string]struct{}
	subscribeIDs         map[string]struct{}
	wsClient             *protocol.WebSocketClient
	mu                   sync.Mutex
	subscribed           bool
	msgBuffer            []*types.Message
	msgBufferBytes       int
	msgBufferMaxMessages int
	msgBufferMaxBytes    int
	msgBufferDropPolicy  string
	wsPendingMu          sync.Mutex
	wsPending            map[string]chan wsRPCResponse
	messageFormat        string
	useRawSubscribe      bool
	subscribePayloads    [][]byte
	skipSubscribe        bool
	skipAckResults       bool
	defaultStreams       []string
	staticMetadata       map[string]any
	notifyMethod         string
	extractBlockMetadata bool
	resultMetadata       map[string]string
	sharedHub            *sharedWebSocketHub
	sharedSubID          int
	sharedMessageCh      <-chan []byte
	releaseWSClient      func()
	shareByEndpoint      bool
	allowedStreams       map[string]struct{}
	backpressureHighPct  int
	backpressureLowPct   int
	messageBackpressure  bool
	wsBoundedBuffer      bool
}

var wsGlobalReqCounter int64

func (w *WebSocketCall) sendSubscribe() error {
	if w.wsClient == nil {
		return fmt.Errorf("websocket client 未初始化")
	}
	if w.skipSubscribe {
		w.subscribed = true
		w.clearSubscriptionRoutingStateLocked()
		w.syncSharedHubRoutesLocked()
		return nil
	}
	if w.useRawSubscribe {
		if len(w.subscribePayloads) == 0 {
			return fmt.Errorf("subscribe payload 为空")
		}
		w.clearSubscriptionRoutingStateLocked()
		tracked, missing := w.trackSubscribeRequestIDsFromRawPayloadsLocked(w.subscribePayloads)
		if w.shareByEndpoint && w.messageFormat == "jsonrpc" && tracked == 0 {
			return fmt.Errorf("share_by_endpoint 模式下 jsonrpc subscribe_raw 必须包含 id 字段")
		}
		if missing > 0 && w.shareByEndpoint && w.messageFormat == "jsonrpc" {
			logging.Warn(context.Background(), logging.EventWSSubscribeParseErr, "jsonrpc subscribe_raw 存在缺失 id 的请求，可能影响共享路由", logging.Fields{
				"missing_count": missing,
				"payload_count": len(w.subscribePayloads),
			})
		}
		w.syncSharedHubRoutesLocked()
		return w.wsClient.SendRawSubscribes(w.subscribePayloads)
	}
	req := w.buildSubscribeRequest(w.subscribeTopic, w.subscribePayload)
	w.subscribeReq = req
	w.clearSubscriptionRoutingStateLocked()
	w.trackSubscribeRequestIDLocked(normalizeRequestIDValue(req.ID))
	w.syncSharedHubRoutesLocked()
	return w.wsClient.Subscribe(req)
}

func (w *WebSocketCall) CallOnce(ctx context.Context, args map[string]any) ([]*types.Message, error) {
	if isRPCRequest(args) {
		return w.executeRPC(ctx, args)
	}

	if err := w.ensureConnected(); err != nil {
		logging.Warn(ctx, logging.EventWSConnectPending, "websocket connecting, waiting for retry", logging.Fields{
			"error": err.Error(),
		})
		return nil, nil
	}

	w.updateSubscribeFromArgs(args)

	w.mu.Lock()
	if !w.subscribed {
		if err := w.sendSubscribe(); err != nil {
			w.mu.Unlock()
			return nil, fmt.Errorf("websocket 订阅失败: %w", err)
		}
		w.subscribed = true
		if !w.skipSubscribe {
			logging.Info(ctx, logging.EventWSSubscribeRequested, "websocket subscribe requested", logging.Fields{
				"subscription": w.subscribeTopic,
			})
		}
	}

	msgs := w.msgBuffer
	w.msgBuffer = make([]*types.Message, 0)
	w.msgBufferBytes = 0
	w.mu.Unlock()

	return msgs, nil
}

func (w *WebSocketCall) Close() error {
	if w.sharedHub != nil && w.sharedSubID > 0 {
		w.sharedHub.unsubscribe(w.sharedSubID)
		w.sharedSubID = 0
	}
	if w.releaseWSClient != nil {
		w.releaseWSClient()
		w.releaseWSClient = nil
		return nil
	}
	if w.wsClient != nil {
		return w.wsClient.Close()
	}
	return nil
}

func isRPCRequest(args map[string]any) bool {
	if len(args) == 0 {
		return false
	}
	if v, ok := args["rpc"]; ok {
		return toBool(v, false)
	}
	if method := util.ToString(args["rpc_method"]); method != "" {
		return true
	}
	if method := util.ToString(args["method"]); method != "" {
		if _, hasParams := args["params"]; hasParams {
			if _, hasSubscribe := args["subscribe"]; hasSubscribe {
				return false
			}
			if _, hasStreams := args["streams"]; hasStreams {
				return false
			}
			return true
		}
	}
	return false
}

func (w *WebSocketCall) ensureConnected() error {
	if w.wsClient == nil {
		return fmt.Errorf("websocket client 未初始化")
	}
	if w.wsClient.IsConnected() {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.wsClient.IsConnected() {
		return nil
	}
	if err := w.wsClient.Connect(); err != nil {
		return err
	}
	w.subscribed = false
	w.clearSubscriptionRoutingStateLocked()
	w.syncSharedHubRoutesLocked()
	return nil
}

func (w *WebSocketCall) updateSubscribeFromArgs(args map[string]any) {
	if !hasSubscribeOverrides(args) {
		return
	}
	w.mu.Lock()
	w.refreshSubscribeRequestLocked(args)
	w.subscribed = false
	w.clearSubscriptionRoutingStateLocked()
	w.syncSharedHubRoutesLocked()
	w.mu.Unlock()
}

func hasSubscribeOverrides(args map[string]any) bool {
	if args == nil || len(args) == 0 {
		return false
	}
	keys := []string{
		"subscribe",
		"subscribe_raw",
		"subscribe_method",
		"subscribe_jsonrpc",
		"subscribe_params",
		"subscribe_id",
		"subscribe_extra",
		"streams",
		"listen_key",
		"message_format",
		"notify_method",
		"metadata_fields",
		"static_metadata",
	}
	for _, k := range keys {
		if _, ok := args[k]; ok {
			return true
		}
	}
	return false
}

func buildEndpointShareKey(url, format string) string {
	return strings.ToLower(strings.TrimSpace(format)) + "|" + strings.TrimSpace(url)
}
