package caller

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func TestNewWebSocketCallSharesEndpointClient(t *testing.T) {
	wsURL, shutdown := startCallerWebSocketServer(t)
	defer shutdown()

	callerConfig := map[string]any{
		"url":            wsURL,
		"message_format": "binance",
	}
	params := map[string]any{
		"caller_params": map[string]any{
			"streams": []string{"btcusdt@depth"},
			"reconnect": map[string]any{
				"share_by_endpoint": true,
			},
		},
	}

	callA, err := NewWebSocketCall(callerConfig, params)
	if err != nil {
		t.Fatalf("create callA failed: %v", err)
	}
	defer func() { _ = callA.Close() }()

	callB, err := NewWebSocketCall(callerConfig, params)
	if err != nil {
		t.Fatalf("create callB failed: %v", err)
	}
	defer func() { _ = callB.Close() }()

	if callA.wsClient != callB.wsClient {
		t.Fatalf("expected shared websocket client for same endpoint")
	}
	if callA.sharedHub != callB.sharedHub {
		t.Fatalf("expected shared websocket hub for same endpoint")
	}
}

func startCallerWebSocketServer(t *testing.T) (string, func()) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
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
	return wsURL, srv.Close
}
