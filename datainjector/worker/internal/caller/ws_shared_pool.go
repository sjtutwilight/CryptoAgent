package caller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/logging"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/metrics"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/protocol"
)

type sharedWSSubscriber struct {
	ch     chan []byte
	routes map[string]struct{}
}

type sharedWebSocketHub struct {
	key    string
	format string
	client *protocol.WebSocketClient
	// 仅在 endpoint 复用时启用路由分发；非复用场景保留全量广播语义。
	routeDispatchEnabled bool

	mu          sync.RWMutex
	subscribers map[int]*sharedWSSubscriber
	routeIndex  map[string]map[int]struct{}
	nextSubID   int
	closed      bool
	done        chan struct{}
}

func newSharedWebSocketHub(key string, client *protocol.WebSocketClient, routeDispatchEnabled bool) *sharedWebSocketHub {
	h := &sharedWebSocketHub{
		key:         key,
		format:      parseSharedHubFormat(key),
		client:      client,
		routeDispatchEnabled: routeDispatchEnabled,
		subscribers: make(map[int]*sharedWSSubscriber),
		routeIndex:  make(map[string]map[int]struct{}),
		done:        make(chan struct{}),
	}
	go h.dispatchLoop()
	return h
}

func (h *sharedWebSocketHub) dispatchLoop() {
	if h == nil || h.client == nil {
		return
	}
	source := h.client.MessageChan()
	for {
		select {
		case <-h.done:
			return
		case data, ok := <-source:
			if !ok {
				return
			}
			if len(data) == 0 {
				continue
			}
			h.broadcast(data)
		}
	}
}

func (h *sharedWebSocketHub) broadcast(data []byte) {
	routeKey := ""
	routed := false
	if h.routeDispatchEnabled {
		routeKey, routed = h.extractRouteKey(data)
	}
	targets := h.snapshotTargets(routeKey, routed)
	if len(targets) == 0 {
		return
	}
	for id, ch := range targets {
		if ch == nil {
			continue
		}
		payload := make([]byte, len(data))
		copy(payload, data)
		select {
		case ch <- payload:
		default:
			logging.Warn(context.Background(), logging.EventWSBufferDrop, "shared websocket subscriber buffer full, dropping message", logging.Fields{
				"ws_key":       h.key,
				"subscriber":   id,
				"buffer_cap":   cap(ch),
				"route_key":    routeKey,
				"drop_context": "shared_hub_dispatch",
				"buffer_layer": "shared_hub_dispatch",
				"drop_reason":  "subscriber_channel_full",
			})
			metrics.RecordWebSocketDrop("", "shared_hub_dispatch", "subscriber_channel_full")
		}
	}
}

func (h *sharedWebSocketHub) subscribe(bufferSize int, routes []string) (int, <-chan []byte, error) {
	if bufferSize <= 0 {
		bufferSize = 1024
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, nil, fmt.Errorf("shared websocket hub closed")
	}
	h.nextSubID++
	id := h.nextSubID
	ch := make(chan []byte, bufferSize)
	h.subscribers[id] = &sharedWSSubscriber{
		ch:     ch,
		routes: make(map[string]struct{}),
	}
	h.replaceRoutesLocked(id, routes)
	return id, ch, nil
}

func (h *sharedWebSocketHub) unsubscribe(id int) {
	if id <= 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	sub, ok := h.subscribers[id]
	if !ok {
		return
	}
	h.removeSubscriberRoutesLocked(id)
	delete(h.subscribers, id)
	close(sub.ch)
}

func (h *sharedWebSocketHub) replaceRoutes(id int, routes []string) {
	if id <= 0 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.replaceRoutesLocked(id, routes)
}

func (h *sharedWebSocketHub) bindRoute(id int, route string) {
	if id <= 0 {
		return
	}
	normalized := normalizeHubRouteKey(h.format, route)
	if normalized == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	sub, ok := h.subscribers[id]
	if !ok || sub == nil {
		return
	}
	if sub.routes == nil {
		sub.routes = make(map[string]struct{})
	}
	if _, exists := sub.routes[normalized]; exists {
		return
	}
	sub.routes[normalized] = struct{}{}
	if h.routeIndex[normalized] == nil {
		h.routeIndex[normalized] = make(map[int]struct{})
	}
	h.routeIndex[normalized][id] = struct{}{}
}

func (h *sharedWebSocketHub) replaceRoutesLocked(id int, routes []string) {
	sub, ok := h.subscribers[id]
	if !ok || sub == nil {
		return
	}
	h.removeSubscriberRoutesLocked(id)
	sub.routes = make(map[string]struct{})
	for _, route := range routes {
		normalized := normalizeHubRouteKey(h.format, route)
		if normalized == "" {
			continue
		}
		sub.routes[normalized] = struct{}{}
		if h.routeIndex[normalized] == nil {
			h.routeIndex[normalized] = make(map[int]struct{})
		}
		h.routeIndex[normalized][id] = struct{}{}
	}
}

func (h *sharedWebSocketHub) removeSubscriberRoutesLocked(id int) {
	sub, ok := h.subscribers[id]
	if !ok || sub == nil || len(sub.routes) == 0 {
		return
	}
	for route := range sub.routes {
		index := h.routeIndex[route]
		if len(index) == 0 {
			continue
		}
		delete(index, id)
		if len(index) == 0 {
			delete(h.routeIndex, route)
		}
	}
	sub.routes = make(map[string]struct{})
}

func (h *sharedWebSocketHub) snapshotTargets(routeKey string, routed bool) map[int]chan []byte {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.subscribers) == 0 {
		return nil
	}

	if !routed || routeKey == "" {
		targets := make(map[int]chan []byte, len(h.subscribers))
		for id, sub := range h.subscribers {
			if sub == nil || sub.ch == nil {
				continue
			}
			targets[id] = sub.ch
		}
		return targets
	}

	ids := h.routeIndex[routeKey]
	if len(ids) == 0 {
		return nil
	}
	targets := make(map[int]chan []byte, len(ids))
	for id := range ids {
		sub, ok := h.subscribers[id]
		if !ok || sub == nil || sub.ch == nil {
			continue
		}
		targets[id] = sub.ch
	}
	return targets
}

func (h *sharedWebSocketHub) extractRouteKey(data []byte) (string, bool) {
	switch h.format {
	case "binance":
		var payload struct {
			Stream string `json:"stream"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return "", false
		}
		route := normalizeHubRouteKey(h.format, payload.Stream)
		if route == "" {
			return "", false
		}
		return route, true
	case "hyperliquid":
		var payload struct {
			Channel string `json:"channel"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return "", false
		}
		route := normalizeHubRouteKey(h.format, payload.Channel)
		if route == "" {
			return "", false
		}
		return route, true
	default:
		var payload struct {
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			return "", false
		}
		if len(payload.Params) == 0 {
			return "", false
		}
		var params struct {
			Subscription string `json:"subscription"`
		}
		if err := json.Unmarshal(payload.Params, &params); err != nil {
			return "", false
		}
		route := normalizeHubRouteKey(h.format, params.Subscription)
		if route == "" {
			return "", false
		}
		return route, true
	}
}

func (h *sharedWebSocketHub) close() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	close(h.done)
	for id, sub := range h.subscribers {
		h.removeSubscriberRoutesLocked(id)
		delete(h.subscribers, id)
		if sub != nil && sub.ch != nil {
			close(sub.ch)
		}
	}
	h.mu.Unlock()
}

func parseSharedHubFormat(key string) string {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return "jsonrpc"
	}
	parts := strings.SplitN(trimmed, "|", 2)
	if len(parts) == 0 {
		return "jsonrpc"
	}
	format := strings.ToLower(strings.TrimSpace(parts[0]))
	if format == "" {
		return "jsonrpc"
	}
	return format
}

func normalizeHubRouteKey(format string, route string) string {
	key := strings.TrimSpace(route)
	if key == "" {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "binance", "hyperliquid":
		return strings.ToLower(key)
	default:
		return key
	}
}

type sharedWSEntry struct {
	client *protocol.WebSocketClient
	hub    *sharedWebSocketHub
	refs   int
}

var wsEndpointPool = struct {
	mu      sync.Mutex
	entries map[string]*sharedWSEntry
}{
	entries: make(map[string]*sharedWSEntry),
}

func acquireWebSocketEndpoint(
	key string,
	cfg protocol.WebSocketConfig,
	share bool,
) (*protocol.WebSocketClient, *sharedWebSocketHub, func(), error) {
	if !share {
		client := protocol.NewWebSocketClient(cfg)
		hub := newSharedWebSocketHub(key, client, false)
		var once sync.Once
		release := func() {
			once.Do(func() {
				hub.close()
				_ = client.Close()
			})
		}
		return client, hub, release, nil
	}

	wsEndpointPool.mu.Lock()
	entry, ok := wsEndpointPool.entries[key]
	if ok {
		entry.refs++
		wsEndpointPool.mu.Unlock()
		var once sync.Once
		release := func() {
			once.Do(func() {
				releaseSharedEndpoint(key)
			})
		}
		return entry.client, entry.hub, release, nil
	}

	client := protocol.NewWebSocketClient(cfg)
	hub := newSharedWebSocketHub(key, client, true)
	entry = &sharedWSEntry{
		client: client,
		hub:    hub,
		refs:   1,
	}
	wsEndpointPool.entries[key] = entry
	wsEndpointPool.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			releaseSharedEndpoint(key)
		})
	}
	return client, hub, release, nil
}

func releaseSharedEndpoint(key string) {
	wsEndpointPool.mu.Lock()
	entry, ok := wsEndpointPool.entries[key]
	if !ok {
		wsEndpointPool.mu.Unlock()
		return
	}
	entry.refs--
	if entry.refs > 0 {
		wsEndpointPool.mu.Unlock()
		return
	}
	delete(wsEndpointPool.entries, key)
	wsEndpointPool.mu.Unlock()

	entry.hub.close()
	_ = entry.client.Close()
}
