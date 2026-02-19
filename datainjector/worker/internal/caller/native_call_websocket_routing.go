package caller

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func (w *WebSocketCall) setAllowedStreams(streams []string) {
	w.mu.Lock()
	w.setAllowedStreamsLocked(streams)
	w.mu.Unlock()
}

func (w *WebSocketCall) setAllowedStreamsLocked(streams []string) {
	if len(streams) == 0 {
		w.allowedStreams = nil
		return
	}
	allowed := make(map[string]struct{}, len(streams))
	for _, stream := range streams {
		normalized := strings.ToLower(strings.TrimSpace(stream))
		if normalized == "" {
			continue
		}
		allowed[normalized] = struct{}{}
	}
	if len(allowed) == 0 {
		w.allowedStreams = nil
		return
	}
	w.allowedStreams = allowed
}

func (w *WebSocketCall) shouldDropByStream(stream string) bool {
	normalized := strings.ToLower(strings.TrimSpace(stream))
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.allowedStreams) == 0 {
		return false
	}
	_, ok := w.allowedStreams[normalized]
	return !ok
}

func (w *WebSocketCall) clearSubscriptionRoutingState() {
	w.mu.Lock()
	w.clearSubscriptionRoutingStateLocked()
	w.mu.Unlock()
}

func (w *WebSocketCall) clearSubscriptionRoutingStateLocked() {
	if len(w.subscribeReqIDs) > 0 {
		for k := range w.subscribeReqIDs {
			delete(w.subscribeReqIDs, k)
		}
	}
	if len(w.subscribeIDs) > 0 {
		for k := range w.subscribeIDs {
			delete(w.subscribeIDs, k)
		}
	}
}

func (w *WebSocketCall) trackSubscribeRequestID(id string) {
	if id == "" {
		return
	}
	w.mu.Lock()
	w.trackSubscribeRequestIDLocked(id)
	w.mu.Unlock()
}

func (w *WebSocketCall) trackSubscribeRequestIDsFromRawPayloads(payloads [][]byte) (tracked int, missing int) {
	w.mu.Lock()
	tracked, missing = w.trackSubscribeRequestIDsFromRawPayloadsLocked(payloads)
	w.mu.Unlock()
	return tracked, missing
}

func (w *WebSocketCall) trackSubscribeRequestIDLocked(id string) {
	if id == "" {
		return
	}
	if w.subscribeReqIDs == nil {
		w.subscribeReqIDs = make(map[string]struct{})
	}
	w.subscribeReqIDs[id] = struct{}{}
}

func (w *WebSocketCall) trackSubscribeRequestIDsFromRawPayloadsLocked(payloads [][]byte) (tracked int, missing int) {
	for _, payload := range payloads {
		reqID, ok := extractRequestIDFromRawPayload(payload)
		if !ok {
			missing++
			continue
		}
		w.trackSubscribeRequestIDLocked(reqID)
		tracked++
	}
	return tracked, missing
}

func (w *WebSocketCall) isPendingSubscribeRequestID(id string) bool {
	if id == "" {
		return false
	}
	w.mu.Lock()
	_, ok := w.subscribeReqIDs[id]
	w.mu.Unlock()
	return ok
}

func (w *WebSocketCall) shouldDropBySubscription(subscription string) bool {
	subscription = strings.TrimSpace(subscription)
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.subscribeIDs) == 0 {
		return w.shareByEndpoint
	}
	if subscription == "" {
		return w.shareByEndpoint
	}
	_, ok := w.subscribeIDs[subscription]
	return !ok
}

func (w *WebSocketCall) recordSubscriptionAck(reqID string, subID string) bool {
	subID = strings.TrimSpace(subID)
	if subID == "" {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if reqID != "" {
		if len(w.subscribeReqIDs) == 0 {
			if w.shareByEndpoint {
				return false
			}
		} else {
			if _, ok := w.subscribeReqIDs[reqID]; !ok {
				if w.shareByEndpoint {
					return false
				}
			} else {
				delete(w.subscribeReqIDs, reqID)
			}
		}
	}
	if w.subscribeIDs == nil {
		w.subscribeIDs = make(map[string]struct{})
	}
	w.subscribeIDs[subID] = struct{}{}
	if w.sharedHub != nil && w.sharedSubID > 0 {
		w.sharedHub.bindRoute(w.sharedSubID, subID)
	}
	return true
}

func extractRequestIDFromRawPayload(payload []byte) (string, bool) {
	if len(payload) == 0 {
		return "", false
	}
	var obj map[string]any
	if err := json.Unmarshal(payload, &obj); err != nil {
		return "", false
	}
	if len(obj) == 0 {
		return "", false
	}
	id, ok := obj["id"]
	if !ok {
		return "", false
	}
	reqID := normalizeRequestIDValue(id)
	if reqID == "" {
		return "", false
	}
	return reqID, true
}

func normalizeRequestIDValue(id interface{}) string {
	switch v := id.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(v)
	case json.Number:
		return strings.TrimSpace(v.String())
	case float64:
		return strconv.FormatInt(int64(v), 10)
	case float32:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.Itoa(v)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}

func (w *WebSocketCall) syncSharedHubRoutes() {
	w.mu.Lock()
	w.syncSharedHubRoutesLocked()
	w.mu.Unlock()
}

func (w *WebSocketCall) syncSharedHubRoutesLocked() {
	if w.sharedHub == nil || w.sharedSubID <= 0 {
		return
	}
	routes := w.currentSharedHubRoutesLocked()
	w.sharedHub.replaceRoutes(w.sharedSubID, routes)
}

func (w *WebSocketCall) currentSharedHubRoutesLocked() []string {
	switch strings.ToLower(strings.TrimSpace(w.messageFormat)) {
	case "binance", "hyperliquid":
		if len(w.allowedStreams) == 0 {
			return nil
		}
		routes := make([]string, 0, len(w.allowedStreams))
		for stream := range w.allowedStreams {
			routes = append(routes, stream)
		}
		return routes
	default:
		if len(w.subscribeIDs) == 0 {
			return nil
		}
		routes := make([]string, 0, len(w.subscribeIDs))
		for subID := range w.subscribeIDs {
			routes = append(routes, subID)
		}
		return routes
	}
}
