package protocol

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestConnectIsIdempotent(t *testing.T) {
	wsURL, acceptCount, shutdown := startWebSocketTestServer(t)
	defer shutdown()

	client := NewWebSocketClient(WebSocketConfig{
		URL:         wsURL,
		HeartbeatMs: 10_000,
	})
	defer func() {
		_ = client.Close()
	}()

	if err := client.Connect(); err != nil {
		t.Fatalf("first connect failed: %v", err)
	}
	if err := client.Connect(); err != nil {
		t.Fatalf("second connect failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	if got := acceptCount.Load(); got != 1 {
		t.Fatalf("expected 1 accepted websocket connection, got %d", got)
	}
}

func TestStaleConnectionErrorDoesNotReplaceNewConnection(t *testing.T) {
	wsURL, _, shutdown := startWebSocketTestServer(t)
	defer shutdown()

	client := NewWebSocketClient(WebSocketConfig{
		URL:                wsURL,
		HeartbeatMs:        10_000,
		BackoffBaseSeconds: 1,
		BackoffMaxSeconds:  2,
	})
	defer func() {
		_ = client.Close()
	}()

	if err := client.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	oldConn := client.currentConn()
	if oldConn == nil {
		t.Fatalf("expected active connection after connect")
	}

	newConn, _, err := client.newDialer().Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial replacement connection failed: %v", err)
	}
	client.configureConn(newConn)

	client.mu.Lock()
	client.conn = newConn
	client.mu.Unlock()

	_ = oldConn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if client.currentConn() == newConn {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("new active connection was replaced unexpectedly")
}

func TestSendHeartbeatSkipsWhenReconnectInProgress(t *testing.T) {
	client := NewWebSocketClient(WebSocketConfig{
		URL:         "ws://example.invalid/ws",
		HeartbeatMs: 30_000,
	})

	client.reconnecting.Store(true)
	attempted, err := client.sendHeartbeat(nil)
	if err != nil {
		t.Fatalf("unexpected error when reconnecting: %v", err)
	}
	if attempted {
		t.Fatalf("expected heartbeat to be skipped while reconnecting")
	}

	client.reconnecting.Store(false)
	attempted, err = client.sendHeartbeat(nil)
	if !attempted {
		t.Fatalf("expected heartbeat write attempt when not reconnecting")
	}
	if err == nil || !strings.Contains(err.Error(), "websocket未连接") {
		t.Fatalf("expected websocket未连接 error, got: %v", err)
	}
}

func TestDialerCompressionDisabledAndReadDeadlineApplied(t *testing.T) {
	wsURL, _, shutdown := startSilentWebSocketServer(t)
	defer shutdown()

	client := NewWebSocketClient(WebSocketConfig{
		URL:         wsURL,
		HeartbeatMs: 50,
	})

	dialer := client.newDialer()
	if dialer.EnableCompression {
		t.Fatalf("expected websocket compression disabled by default")
	}

	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client.configureConn(conn)

	start := time.Now()
	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Fatalf("expected read deadline timeout error, got nil")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("read timeout took too long: %v", elapsed)
	}
}

func TestSendRawSubscribeDedupesWithinWindow(t *testing.T) {
	wsURL, _, msgCount, shutdown := startCountingWebSocketServer(t)
	defer shutdown()

	client := NewWebSocketClient(WebSocketConfig{
		URL:                     wsURL,
		HeartbeatMs:             10_000,
		SubscribeDedupeWindowMs: 60_000,
	})
	defer func() {
		_ = client.Close()
	}()

	if err := client.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	payload1 := []byte(`{"method":"SUBSCRIBE","params":["btcusdt@depth"],"id":1}`)
	payload2 := []byte(`{"method":"SUBSCRIBE","params":["btcusdt@depth"],"id":2}`)
	if err := client.SendRawSubscribe(payload1); err != nil {
		t.Fatalf("first subscribe failed: %v", err)
	}
	if err := client.SendRawSubscribe(payload2); err != nil {
		t.Fatalf("second subscribe failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := msgCount.Load(); got == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected deduped subscribe count=1, got=%d", msgCount.Load())
}

func TestNextReconnectDelayHonorsMinInterval(t *testing.T) {
	client := NewWebSocketClient(WebSocketConfig{
		URL:                    "ws://example.invalid/ws",
		BackoffBaseSeconds:     2,
		BackoffMaxSeconds:      5,
		BackoffJitterPercent:   0,
		MinReconnectIntervalMs: 5000,
	})

	now := time.Now()
	client.lastReconnectDialAt = now.Add(-1 * time.Second)
	delay := client.nextReconnectDelay(2*time.Second, now)
	if delay < 3900*time.Millisecond || delay > 4100*time.Millisecond {
		t.Fatalf("expected delay close to 4s due to min interval, got %v", delay)
	}
}

func TestRecordReconnectFailureTriggersPolicyCooldown(t *testing.T) {
	client := NewWebSocketClient(WebSocketConfig{
		URL:                      "ws://example.invalid/ws",
		PolicyViolationThreshold: 2,
		PolicyCooldownSeconds:    30,
	})
	now := time.Now()
	err := errors.New("websocket: close 1008 (policy violation) too many requests")

	client.recordReconnectFailure(err, now)
	if client.policyViolationHits != 1 {
		t.Fatalf("expected policy hits=1, got %d", client.policyViolationHits)
	}
	if !client.policyCooldownUntil.IsZero() {
		t.Fatalf("cooldown should not start before reaching threshold")
	}

	client.recordReconnectFailure(err, now.Add(1*time.Second))
	if client.policyViolationHits != 0 {
		t.Fatalf("expected hits reset after cooldown, got %d", client.policyViolationHits)
	}
	if !client.policyCooldownUntil.After(now) {
		t.Fatalf("expected cooldown to be set after threshold")
	}
	if remain := client.policyCooldownRemaining(now.Add(2 * time.Second)); remain <= 0 {
		t.Fatalf("expected positive cooldown remaining, got %v", remain)
	}
}

func startWebSocketTestServer(t *testing.T) (string, *atomic.Int32, func()) {
	t.Helper()

	var accepted atomic.Int32
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		accepted.Add(1)
		go func() {
			defer conn.Close()
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
	}))

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	return wsURL, &accepted, srv.Close
}

func startCountingWebSocketServer(t *testing.T) (string, *atomic.Int32, *atomic.Int32, func()) {
	t.Helper()

	var accepted atomic.Int32
	var msgCount atomic.Int32
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		accepted.Add(1)
		go func() {
			defer conn.Close()
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
				msgCount.Add(1)
			}
		}()
	}))

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	return wsURL, &accepted, &msgCount, srv.Close
}

func startSilentWebSocketServer(t *testing.T) (string, *atomic.Int32, func()) {
	t.Helper()

	var accepted atomic.Int32
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		accepted.Add(1)
		go func() {
			// 保持连接不主动发送任何消息，用于验证客户端 read deadline。
			defer conn.Close()
			<-time.After(3 * time.Second)
		}()
	}))

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	return wsURL, &accepted, srv.Close
}
