package protocol

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/logging"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/metrics"
)

// WebSocketConfig websocket连接配置
type WebSocketConfig struct {
	URL                      string // ws://localhost:8090/ws
	HeartbeatMs              int    // 心跳间隔(毫秒)
	BackoffBaseSeconds       int    // 重连基础延迟(秒)
	BackoffMaxSeconds        int    // 重连最大延迟(秒)
	MinReconnectIntervalMs   int    // 最小重连间隔(毫秒)
	BackoffJitterPercent     int    // 重连抖动百分比(0-100)
	PolicyViolationThreshold int    // 连续1008达到阈值后触发冷静期
	PolicyCooldownSeconds    int    // 策略违规冷静期(秒)
	SubscribeDedupeWindowMs  int    // 订阅去重窗口(毫秒)
	HeartbeatPayload         []byte // 自定义心跳载荷（留空则发送 Ping frame）
	HeartbeatOpcode          int    // websocket 消息类型，默认 websocket.PingMessage
}

// WebSocketClient websocket客户端，负责连接管理、心跳、重连
type WebSocketClient struct {
	config       WebSocketConfig
	conn         *websocket.Conn
	mu           sync.RWMutex
	writeMu      sync.Mutex
	reconnectMu  sync.Mutex
	loopsStarted bool
	reconnecting atomic.Bool

	msgChan chan []byte   // 接收到的消息通道
	errChan chan error    // 错误通道
	closeCh chan struct{} // 关闭信号
	subMu   sync.Mutex    // 订阅消息记录锁
	unsubMu sync.Mutex    // 退订消息记录锁

	lastSubscribeMsgs   [][]byte
	lastUnsubscribeMsgs [][]byte
	desiredSubscribeMap map[string][]byte
	subscribeSentAt     map[string]time.Time
	lastReconnectDialAt time.Time
	policyViolationHits int
	policyCooldownUntil time.Time
	rngMu               sync.Mutex
	rng                 *rand.Rand

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWebSocketClient 创建websocket客户端
func NewWebSocketClient(cfg WebSocketConfig) *WebSocketClient {
	ctx, cancel := context.WithCancel(context.Background())

	// 设置默认值
	if cfg.HeartbeatMs <= 0 {
		cfg.HeartbeatMs = 30000
	}
	if cfg.BackoffBaseSeconds <= 0 {
		cfg.BackoffBaseSeconds = 2
	}
	if cfg.BackoffMaxSeconds <= 0 {
		cfg.BackoffMaxSeconds = 60
	}
	if cfg.MinReconnectIntervalMs <= 0 {
		cfg.MinReconnectIntervalMs = 3000
	}
	if cfg.BackoffJitterPercent <= 0 {
		cfg.BackoffJitterPercent = 30
	}
	if cfg.BackoffJitterPercent > 100 {
		cfg.BackoffJitterPercent = 100
	}
	if cfg.PolicyViolationThreshold <= 0 {
		cfg.PolicyViolationThreshold = 3
	}
	if cfg.PolicyCooldownSeconds <= 0 {
		cfg.PolicyCooldownSeconds = 60
	}
	if cfg.SubscribeDedupeWindowMs <= 0 {
		cfg.SubscribeDedupeWindowMs = 10000
	}
	if cfg.HeartbeatOpcode == 0 {
		cfg.HeartbeatOpcode = websocket.PingMessage
	}

	return &WebSocketClient{
		config:              cfg,
		msgChan:             make(chan []byte, 100),
		errChan:             make(chan error, 10),
		closeCh:             make(chan struct{}),
		desiredSubscribeMap: make(map[string][]byte),
		subscribeSentAt:     make(map[string]time.Time),
		rng:                 rand.New(rand.NewSource(time.Now().UnixNano())),
		ctx:                 ctx,
		cancel:              cancel,
	}
}

// Connect 建立websocket连接并启动心跳、读取协程
func (c *WebSocketClient) Connect() error {
	c.mu.RLock()
	if c.conn != nil {
		c.mu.RUnlock()
		return nil
	}
	c.mu.RUnlock()

	if c.reconnecting.Load() {
		return fmt.Errorf("websocket重连中")
	}

	conn, _, err := c.newDialer().Dial(c.config.URL, nil)
	if err != nil {
		return fmt.Errorf("websocket连接失败: %w", err)
	}
	c.configureConn(conn)

	c.mu.Lock()
	// 双重检查，避免并发 Connect 导致重复连接与重复循环。
	if c.conn != nil {
		c.mu.Unlock()
		_ = conn.Close()
		return nil
	}
	c.conn = conn
	startLoops := !c.loopsStarted
	if startLoops {
		c.loopsStarted = true
	}
	c.mu.Unlock()

	logging.Info(context.Background(), logging.EventWSConnect, "websocket connected", logging.Fields{
		"ws_url": c.config.URL,
	})

	if startLoops {
		// 启动心跳
		c.wg.Add(1)
		go c.heartbeatLoop()

		// 启动读取消息
		c.wg.Add(1)
		go c.readLoop()
	}

	return nil
}

// JSONRPCRequest 通用 JSON-RPC 请求结构
// Subscribe 发送订阅消息（JSON-RPC）
func (c *WebSocketClient) Subscribe(req JSONRPCRequest) error {
	if req.JSONRPC == "" {
		req.JSONRPC = "2.0"
	}
	if req.Method == "" {
		return fmt.Errorf("websocket订阅缺少method")
	}

	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("序列化订阅消息失败: %w", err)
	}
	return c.sendSubscribePayload(data, true, false)
}

// SendRawSubscribe 发送原生订阅payload，并记录用于重连
func (c *WebSocketClient) SendRawSubscribe(payload []byte) error {
	if len(payload) == 0 {
		return fmt.Errorf("订阅消息为空")
	}
	return c.sendSubscribePayload(payload, false, true)
}

// SendRawSubscribes 批量发送原生订阅，并记录以便重连
func (c *WebSocketClient) SendRawSubscribes(payloads [][]byte) error {
	if len(payloads) == 0 {
		return fmt.Errorf("订阅消息为空")
	}
	first := true
	for _, payload := range payloads {
		if len(payload) == 0 {
			continue
		}
		if err := c.sendSubscribePayload(payload, first, true); err != nil {
			return err
		}
		first = false
	}
	return nil
}

// Unsubscribe 发送退订消息（JSON-RPC）
func (c *WebSocketClient) Unsubscribe(req JSONRPCRequest) error {
	if req.JSONRPC == "" {
		req.JSONRPC = "2.0"
	}
	if req.Method == "" {
		return fmt.Errorf("websocket退订缺少method")
	}

	data, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("序列化退订消息失败: %w", err)
	}

	if err := c.writeMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("发送退订消息失败: %w", err)
	}

	payloadCopy := make([]byte, len(data))
	copy(payloadCopy, data)
	c.unsubMu.Lock()
	c.lastUnsubscribeMsgs = [][]byte{payloadCopy}
	c.unsubMu.Unlock()

	logging.Info(context.Background(), logging.EventWSUnsubscribeSent, "websocket unsubscribe sent", logging.Fields{
		"ws_url":      c.config.URL,
		"payload":     truncateBytes(data, 256),
		"payload_len": len(data),
	})
	return nil
}

// MessageChan 返回接收消息的通道
func (c *WebSocketClient) MessageChan() <-chan []byte {
	return c.msgChan
}

// ErrorChan 返回错误通道
func (c *WebSocketClient) ErrorChan() <-chan error {
	return c.errChan
}

// Close 关闭websocket连接
func (c *WebSocketClient) Close() error {
	c.cancel()
	c.reconnecting.Store(false)

	c.writeMu.Lock()
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()
	c.writeMu.Unlock()

	close(c.closeCh)
	c.wg.Wait()

	logging.Info(context.Background(), logging.EventWSClose, "websocket closed", logging.Fields{
		"ws_url": c.config.URL,
	})
	return nil
}

// heartbeatLoop 心跳循环，定期发送ping消息
func (c *WebSocketClient) heartbeatLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Duration(c.config.HeartbeatMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.closeCh:
			return
		case <-ticker.C:
			conn := c.currentConn()
			attempted, err := c.sendHeartbeat(conn)
			if !attempted {
				continue
			}
			if err != nil {
				logging.Warn(context.Background(), logging.EventWSHeartbeatError, "websocket heartbeat failed", logging.Fields{
					"ws_url": c.config.URL,
					"error":  err.Error(),
				})
				c.reconnect(conn, err)
			}
		}
	}
}

// readLoop 读取消息循环
func (c *WebSocketClient) readLoop() {
	defer c.wg.Done()
	var readConn *websocket.Conn
	defer func() {
		if r := recover(); r != nil {
			logging.Warn(context.Background(), logging.EventWSReadError, "websocket read panic recovered", logging.Fields{
				"ws_url": c.config.URL,
				"panic":  fmt.Sprintf("%v", r),
			})
			c.reconnect(readConn, nil)
		}
	}()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.closeCh:
			return
		default:
			conn := c.currentConn()
			readConn = conn

			if conn == nil {
				time.Sleep(time.Second)
				continue
			}

			_, data, err := conn.ReadMessage()
			if err != nil {
				logging.Warn(context.Background(), logging.EventWSReadError, "websocket read failed", logging.Fields{
					"ws_url": c.config.URL,
					"error":  err.Error(),
				})
				c.reconnect(conn, err)
				continue
			}

			c.extendReadDeadline(conn)

			// 将消息发送到通道
			select {
			case c.msgChan <- data:
			case <-c.ctx.Done():
				return
			default:
				logging.Warn(context.Background(), logging.EventWSBufferDrop, "websocket message buffer full, dropping message", logging.Fields{
					"ws_url":       c.config.URL,
					"buffer":       cap(c.msgChan),
					"buffer_layer": "protocol_read_loop",
					"drop_reason":  "channel_full",
				})
				metrics.RecordWebSocketDrop("", "protocol_read_loop", "channel_full")
			}
		}
	}
}

// reconnect 重连逻辑，带指数退避、抖动、最小间隔和策略违规冷静期
func (c *WebSocketClient) reconnect(failedConn *websocket.Conn, cause error) {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()
	c.reconnecting.Store(true)
	defer c.reconnecting.Store(false)
	c.recordReconnectFailure(cause, time.Now())

	c.writeMu.Lock()
	c.mu.Lock()
	current := c.conn
	// 仅允许当前失败连接触发重连，避免 stale 连接打断新连接。
	if failedConn != nil && current != failedConn {
		c.mu.Unlock()
		c.writeMu.Unlock()
		return
	}
	// 未指明失败连接时，仅在当前无连接时允许继续重连。
	if failedConn == nil && current != nil {
		c.mu.Unlock()
		c.writeMu.Unlock()
		return
	}
	if current != nil {
		_ = current.Close()
		c.conn = nil
	}
	c.mu.Unlock()
	c.writeMu.Unlock()

	backoff := time.Duration(c.config.BackoffBaseSeconds) * time.Second
	maxBackoff := time.Duration(c.config.BackoffMaxSeconds) * time.Second

	for attempt := 1; ; attempt++ {
		select {
		case <-c.ctx.Done():
			return
		case <-c.closeCh:
			return
		default:
		}

		now := time.Now()
		if wait := c.policyCooldownRemaining(now); wait > 0 {
			logging.Warn(context.Background(), logging.EventWSReconnectCooldown, "websocket reconnect in cooldown", logging.Fields{
				"ws_url":      c.config.URL,
				"cooldown_ms": wait.Milliseconds(),
			})
			if !sleepWithCancel(c.ctx, c.closeCh, wait) {
				return
			}
		}

		delay := c.nextReconnectDelay(backoff, time.Now())
		logging.Info(context.Background(), logging.EventWSReconnectStart, "websocket reconnect attempt", logging.Fields{
			"ws_url":     c.config.URL,
			"attempt":    attempt,
			"backoff_ms": backoff.Milliseconds(),
			"delay_ms":   delay.Milliseconds(),
		})
		if !sleepWithCancel(c.ctx, c.closeCh, delay) {
			return
		}
		c.lastReconnectDialAt = time.Now()

		conn, _, err := c.newDialer().Dial(c.config.URL, nil)
		if err != nil {
			logging.Warn(context.Background(), logging.EventWSReconnectError, "websocket reconnect failed", logging.Fields{
				"ws_url": c.config.URL,
				"error":  err.Error(),
			})
			c.recordReconnectFailure(err, time.Now())
			// 指数退避
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		c.configureConn(conn)

		c.mu.Lock()
		if c.conn != nil {
			c.mu.Unlock()
			_ = conn.Close()
			return
		}
		c.conn = conn
		c.mu.Unlock()

		logging.Info(context.Background(), logging.EventWSReconnectSuccess, "websocket reconnected", logging.Fields{
			"ws_url": c.config.URL,
		})
		c.policyViolationHits = 0

		// 重连后尝试恢复订阅
		subs := c.snapshotDesiredSubscribes()
		for _, sub := range subs {
			if len(sub.Payload) == 0 {
				continue
			}
			if err := c.writeMessage(websocket.TextMessage, sub.Payload); err != nil {
				logging.Warn(context.Background(), logging.EventWSSubscribeRetryErr, "websocket resubscribe failed", logging.Fields{
					"ws_url": c.config.URL,
					"error":  err.Error(),
				})
			} else {
				c.markSubscribeSent(sub.Key, time.Now())
				logging.Info(context.Background(), logging.EventWSSubscribeRetryOK, "websocket resubscribe ok", logging.Fields{
					"ws_url": c.config.URL,
				})
			}
		}
		return
	}
}

type subscribePayload struct {
	Key     string
	Payload []byte
}

func (c *WebSocketClient) sendSubscribePayload(payload []byte, replace bool, raw bool) error {
	key := normalizeSubscribeKey(payload)
	now := time.Now()
	c.recordDesiredSubscribe(key, payload, replace)

	if c.shouldDedupeSubscribe(key, now) {
		logging.Info(context.Background(), logging.EventWSSubscribeDeduped, "websocket subscribe deduped", logging.Fields{
			"ws_url": c.config.URL,
			"raw":    raw,
		})
		return nil
	}

	if err := c.writeMessage(websocket.TextMessage, payload); err != nil {
		return fmt.Errorf("发送订阅消息失败: %w", err)
	}
	c.markSubscribeSent(key, now)

	fields := logging.Fields{
		"ws_url":      c.config.URL,
		"payload":     truncateBytes(payload, 256),
		"payload_len": len(payload),
	}
	if raw {
		fields["raw"] = true
	}
	logging.Info(context.Background(), logging.EventWSSubscribeSent, "websocket subscribe sent", fields)
	return nil
}

func (c *WebSocketClient) shouldDedupeSubscribe(key string, now time.Time) bool {
	window := time.Duration(c.config.SubscribeDedupeWindowMs) * time.Millisecond
	if window <= 0 {
		return false
	}
	c.subMu.Lock()
	defer c.subMu.Unlock()
	last, ok := c.subscribeSentAt[key]
	if !ok || now.Sub(last) > window {
		return false
	}
	return true
}

func (c *WebSocketClient) recordDesiredSubscribe(key string, payload []byte, replace bool) {
	payloadCopy := make([]byte, len(payload))
	copy(payloadCopy, payload)

	c.subMu.Lock()
	defer c.subMu.Unlock()
	if replace {
		c.desiredSubscribeMap = map[string][]byte{}
	}
	c.desiredSubscribeMap[key] = payloadCopy
	c.refreshLastSubscribeMsgsLocked()
}

func (c *WebSocketClient) markSubscribeSent(key string, now time.Time) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	c.subscribeSentAt[key] = now
}

func (c *WebSocketClient) refreshLastSubscribeMsgsLocked() {
	if len(c.desiredSubscribeMap) == 0 {
		c.lastSubscribeMsgs = nil
		return
	}
	keys := make([]string, 0, len(c.desiredSubscribeMap))
	for key := range c.desiredSubscribeMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make([][]byte, 0, len(keys))
	for _, key := range keys {
		payload := c.desiredSubscribeMap[key]
		if len(payload) == 0 {
			continue
		}
		copied := make([]byte, len(payload))
		copy(copied, payload)
		out = append(out, copied)
	}
	c.lastSubscribeMsgs = out
}

func (c *WebSocketClient) snapshotDesiredSubscribes() []subscribePayload {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	if len(c.desiredSubscribeMap) == 0 {
		return nil
	}
	keys := make([]string, 0, len(c.desiredSubscribeMap))
	for key := range c.desiredSubscribeMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]subscribePayload, 0, len(keys))
	for _, key := range keys {
		payload := c.desiredSubscribeMap[key]
		if len(payload) == 0 {
			continue
		}
		copied := make([]byte, len(payload))
		copy(copied, payload)
		out = append(out, subscribePayload{
			Key:     key,
			Payload: copied,
		})
	}
	return out
}

func (c *WebSocketClient) nextReconnectDelay(backoff time.Duration, now time.Time) time.Duration {
	delay := c.applyReconnectJitter(backoff)
	minInterval := time.Duration(c.config.MinReconnectIntervalMs) * time.Millisecond
	if minInterval > 0 && !c.lastReconnectDialAt.IsZero() {
		since := now.Sub(c.lastReconnectDialAt)
		if since < minInterval {
			required := minInterval - since
			if required > delay {
				delay = required
			}
		}
	}
	if delay < 0 {
		return 0
	}
	return delay
}

func (c *WebSocketClient) applyReconnectJitter(backoff time.Duration) time.Duration {
	jitterPercent := c.config.BackoffJitterPercent
	if jitterPercent <= 0 || backoff <= 0 {
		return backoff
	}
	scale := float64(jitterPercent) / 100.0
	minFactor := 1.0 - scale
	maxFactor := 1.0 + scale

	c.rngMu.Lock()
	factor := minFactor + c.rng.Float64()*(maxFactor-minFactor)
	c.rngMu.Unlock()

	delay := time.Duration(float64(backoff) * factor)
	if delay < 0 {
		return 0
	}
	return delay
}

func (c *WebSocketClient) recordReconnectFailure(err error, now time.Time) {
	if !isPolicyViolationErr(err) {
		if err != nil {
			c.policyViolationHits = 0
		}
		return
	}
	c.policyViolationHits++
	logging.Warn(context.Background(), logging.EventWSPolicyViolation, "websocket policy violation detected", logging.Fields{
		"ws_url":      c.config.URL,
		"error":       err.Error(),
		"consecutive": c.policyViolationHits,
	})
	if c.policyViolationHits < c.config.PolicyViolationThreshold {
		return
	}
	c.policyViolationHits = 0
	c.policyCooldownUntil = now.Add(time.Duration(c.config.PolicyCooldownSeconds) * time.Second)
}

func (c *WebSocketClient) policyCooldownRemaining(now time.Time) time.Duration {
	if c.policyCooldownUntil.IsZero() || !now.Before(c.policyCooldownUntil) {
		return 0
	}
	return c.policyCooldownUntil.Sub(now)
}

func normalizeSubscribeKey(payload []byte) string {
	var v any
	if err := json.Unmarshal(payload, &v); err != nil {
		return string(payload)
	}
	if obj, ok := v.(map[string]any); ok {
		delete(obj, "id")
		delete(obj, "jsonrpc")
	}
	normalized, err := json.Marshal(v)
	if err != nil {
		return string(payload)
	}
	return string(normalized)
}

func isPolicyViolationErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "1008") ||
		strings.Contains(msg, "policy violation") ||
		strings.Contains(msg, "too many requests")
}

func sleepWithCancel(ctx context.Context, closeCh <-chan struct{}, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-closeCh:
		return false
	case <-timer.C:
		return true
	}
}

func (c *WebSocketClient) newDialer() *websocket.Dialer {
	d := *websocket.DefaultDialer
	d.HandshakeTimeout = 10 * time.Second
	d.EnableCompression = false
	d.ReadBufferSize = 64 * 1024
	d.WriteBufferSize = 64 * 1024
	return &d
}

func (c *WebSocketClient) currentConn() *websocket.Conn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn
}

func (c *WebSocketClient) readTimeout() time.Duration {
	hb := time.Duration(c.config.HeartbeatMs) * time.Millisecond
	if hb <= 0 {
		hb = 30 * time.Second
	}
	return hb * 2
}

func (c *WebSocketClient) configureConn(conn *websocket.Conn) {
	if conn == nil {
		return
	}
	c.extendReadDeadline(conn)
	timeout := c.readTimeout()
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(timeout))
	})
}

func (c *WebSocketClient) extendReadDeadline(conn *websocket.Conn) {
	if conn == nil {
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(c.readTimeout()))
}

func truncateBytes(data []byte, limit int) string {
	if limit <= 0 || len(data) <= limit {
		return string(data)
	}
	return string(data[:limit])
}

// IsConnected 返回当前是否有活跃连接
func (c *WebSocketClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn != nil
}

// WriteMessage 发送消息到websocket连接
func (c *WebSocketClient) WriteMessage(data []byte) error {
	return c.writeMessage(websocket.TextMessage, data)
}

func (c *WebSocketClient) writeMessage(messageType int, data []byte) error {
	return c.writeMessageWithExpectedConn(nil, messageType, data)
}

func (c *WebSocketClient) writeMessageWithExpectedConn(expected *websocket.Conn, messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()

	if expected != nil && conn != expected {
		return fmt.Errorf("websocket连接已切换")
	}

	if conn == nil {
		return fmt.Errorf("websocket未连接")
	}

	return conn.WriteMessage(messageType, data)
}

func (c *WebSocketClient) sendHeartbeat(conn *websocket.Conn) (bool, error) {
	if c.reconnecting.Load() {
		return false, nil
	}
	if len(c.config.HeartbeatPayload) > 0 {
		return true, c.writeMessageWithExpectedConn(conn, c.config.HeartbeatOpcode, c.config.HeartbeatPayload)
	}
	return true, c.writeMessageWithExpectedConn(conn, websocket.PingMessage, []byte{})
}
