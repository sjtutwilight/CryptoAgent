package caller

import "testing"

func TestSharedHubBroadcastBinanceByStreamRoute(t *testing.T) {
	hub := &sharedWebSocketHub{
		key:         "binance|ws://test",
		format:      "binance",
		routeDispatchEnabled: true,
		subscribers: make(map[int]*sharedWSSubscriber),
		routeIndex:  make(map[string]map[int]struct{}),
		done:        make(chan struct{}),
	}

	_, chDepth, err := hub.subscribe(2, []string{"btcusdt@depth"})
	if err != nil {
		t.Fatalf("subscribe depth failed: %v", err)
	}
	_, chTrade, err := hub.subscribe(2, []string{"btcusdt@aggtrade"})
	if err != nil {
		t.Fatalf("subscribe trade failed: %v", err)
	}

	hub.broadcast([]byte(`{"stream":"btcusdt@depth","data":{"x":1}}`))

	select {
	case <-chDepth:
	default:
		t.Fatalf("expected depth subscriber receive routed message")
	}
	select {
	case <-chTrade:
		t.Fatalf("expected trade subscriber not receive depth message")
	default:
	}
}

func TestSharedHubBroadcastJSONRPCBySubscriptionRoute(t *testing.T) {
	hub := &sharedWebSocketHub{
		key:         "jsonrpc|ws://test",
		format:      "jsonrpc",
		routeDispatchEnabled: true,
		subscribers: make(map[int]*sharedWSSubscriber),
		routeIndex:  make(map[string]map[int]struct{}),
		done:        make(chan struct{}),
	}

	subA, chA, err := hub.subscribe(2, nil)
	if err != nil {
		t.Fatalf("subscribe A failed: %v", err)
	}
	_, chB, err := hub.subscribe(2, nil)
	if err != nil {
		t.Fatalf("subscribe B failed: %v", err)
	}
	hub.bindRoute(subA, "sub-a")

	hub.broadcast([]byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"sub-a","result":{"n":1}}}`))

	select {
	case <-chA:
	default:
		t.Fatalf("expected routed notification delivered to sub-a")
	}
	select {
	case <-chB:
		t.Fatalf("expected sub-b not receive sub-a notification")
	default:
	}
}

func TestSharedHubBroadcastDropsWhenRouteNotMatched(t *testing.T) {
	hub := &sharedWebSocketHub{
		key:         "jsonrpc|ws://test",
		format:      "jsonrpc",
		routeDispatchEnabled: true,
		subscribers: make(map[int]*sharedWSSubscriber),
		routeIndex:  make(map[string]map[int]struct{}),
		done:        make(chan struct{}),
	}

	subA, chA, err := hub.subscribe(2, nil)
	if err != nil {
		t.Fatalf("subscribe A failed: %v", err)
	}
	hub.bindRoute(subA, "sub-a")

	hub.broadcast([]byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"sub-b","result":{"n":1}}}`))

	select {
	case <-chA:
		t.Fatalf("expected sub-a not receive sub-b notification")
	default:
	}
}

func TestSharedHubBroadcastFallbackForNonRoutableMessage(t *testing.T) {
	hub := &sharedWebSocketHub{
		key:         "jsonrpc|ws://test",
		format:      "jsonrpc",
		routeDispatchEnabled: true,
		subscribers: make(map[int]*sharedWSSubscriber),
		routeIndex:  make(map[string]map[int]struct{}),
		done:        make(chan struct{}),
	}

	_, chA, err := hub.subscribe(2, nil)
	if err != nil {
		t.Fatalf("subscribe A failed: %v", err)
	}
	_, chB, err := hub.subscribe(2, nil)
	if err != nil {
		t.Fatalf("subscribe B failed: %v", err)
	}

	// 无 params.subscription，可视为非可路由消息（例如 RPC response），应回退广播。
	hub.broadcast([]byte(`{"jsonrpc":"2.0","id":"123","result":{"ok":true}}`))

	select {
	case <-chA:
	default:
		t.Fatalf("expected subscriber A receive fallback broadcast")
	}
	select {
	case <-chB:
	default:
		t.Fatalf("expected subscriber B receive fallback broadcast")
	}
}
