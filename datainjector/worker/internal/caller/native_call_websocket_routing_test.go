package caller

import (
	"strings"
	"testing"
)

func TestHandleJSONRPCMessageSharedRoutesBySubscriptionID(t *testing.T) {
	call := &WebSocketCall{
		notifyMethod:    "eth_subscription",
		shareByEndpoint: true,
		subscribeReqIDs: make(map[string]struct{}),
		subscribeIDs:    make(map[string]struct{}),
	}
	call.trackSubscribeRequestID("req-1")

	if err := call.handleJSONRPCMessage([]byte(`{"jsonrpc":"2.0","id":"req-1","result":"sub-own"}`)); err != nil {
		t.Fatalf("handle ack failed: %v", err)
	}
	if _, ok := call.subscribeIDs["sub-own"]; !ok {
		t.Fatalf("expected subscription id tracked after ack")
	}

	if err := call.handleJSONRPCMessage([]byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"sub-foreign","result":{"n":1}}}`)); err != nil {
		t.Fatalf("handle foreign notify failed: %v", err)
	}
	if len(call.msgBuffer) != 0 {
		t.Fatalf("expected foreign notification dropped, got %d buffered", len(call.msgBuffer))
	}

	if err := call.handleJSONRPCMessage([]byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"sub-own","result":{"n":2}}}`)); err != nil {
		t.Fatalf("handle own notify failed: %v", err)
	}
	if len(call.msgBuffer) != 1 {
		t.Fatalf("expected own notification buffered, got %d", len(call.msgBuffer))
	}
	if got := call.msgBuffer[0].Metadata["subscription"]; got != "sub-own" {
		t.Fatalf("expected subscription metadata sub-own, got %v", got)
	}
}

func TestHandleJSONRPCMessageSharedIgnoresForeignRPCResponse(t *testing.T) {
	call := &WebSocketCall{
		shareByEndpoint: true,
		notifyMethod:    "eth_subscription",
		subscribeReqIDs: make(map[string]struct{}),
		subscribeIDs:    make(map[string]struct{}),
	}
	err := call.handleJSONRPCMessage([]byte(`{"jsonrpc":"2.0","id":"foreign-req","error":{"code":-32000,"message":"boom"}}`))
	if err != nil {
		t.Fatalf("expected foreign rpc response ignored in shared mode, got: %v", err)
	}
	if len(call.subscribeIDs) != 0 {
		t.Fatalf("expected no subscription id updated by foreign response")
	}
	if len(call.msgBuffer) != 0 {
		t.Fatalf("expected no buffered message, got %d", len(call.msgBuffer))
	}
}

func TestHandleJSONRPCMessageSharedDropsNotifyBeforeAck(t *testing.T) {
	call := &WebSocketCall{
		shareByEndpoint: true,
		notifyMethod:    "eth_subscription",
		subscribeReqIDs: make(map[string]struct{}),
		subscribeIDs:    make(map[string]struct{}),
	}
	if err := call.handleJSONRPCMessage([]byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"sub-any","result":{"n":1}}}`)); err != nil {
		t.Fatalf("handle notify before ack failed: %v", err)
	}
	if len(call.msgBuffer) != 0 {
		t.Fatalf("expected pre-ack notification dropped in shared mode, got %d", len(call.msgBuffer))
	}
}

func TestTrackSubscribeRequestIDsFromRawPayloads(t *testing.T) {
	call := &WebSocketCall{
		subscribeReqIDs: make(map[string]struct{}),
	}
	tracked, missing := call.trackSubscribeRequestIDsFromRawPayloads([][]byte{
		[]byte(`{"id":1,"method":"eth_subscribe","params":["newHeads"]}`),
		[]byte(`{"method":"eth_subscribe","params":["newHeads"]}`),
	})
	if tracked != 1 || missing != 1 {
		t.Fatalf("expected tracked=1 missing=1, got tracked=%d missing=%d", tracked, missing)
	}
	if _, ok := call.subscribeReqIDs["1"]; !ok {
		t.Fatalf("expected tracked request id 1")
	}
}

func TestNextRequestIDStringUsesGlobalUniqueCounter(t *testing.T) {
	callA := &WebSocketCall{
		sharedSubID:     101,
		shareByEndpoint: true,
		messageFormat:   "jsonrpc",
	}
	callB := &WebSocketCall{
		sharedSubID:     202,
		shareByEndpoint: true,
		messageFormat:   "jsonrpc",
	}
	idA := callA.nextRequestIDString()
	idB := callB.nextRequestIDString()
	if idA == idB {
		t.Fatalf("expected unique request ids across callers, got both %s", idA)
	}
	if !strings.Contains(idA, ":") || !strings.Contains(idB, ":") {
		t.Fatalf("expected shared request ids with subscriber prefix, got %s and %s", idA, idB)
	}
}

func TestNextRequestIDStringNoPrefixWithoutEndpointShare(t *testing.T) {
	call := &WebSocketCall{
		sharedSubID:     101,
		shareByEndpoint: false,
		messageFormat:   "jsonrpc",
	}
	id := call.nextRequestIDString()
	if strings.Contains(id, ":") {
		t.Fatalf("expected request id without subscriber prefix when share_by_endpoint disabled, got %s", id)
	}
}

func TestNextRequestIDStringNoPrefixForBinance(t *testing.T) {
	call := &WebSocketCall{
		sharedSubID:     101,
		shareByEndpoint: true,
		messageFormat:   "binance",
	}
	id := call.nextRequestIDString()
	if strings.Contains(id, ":") {
		t.Fatalf("expected request id without subscriber prefix for binance format, got %s", id)
	}
}

func TestRecordSubscriptionAckBindsHubRoute(t *testing.T) {
	hub := &sharedWebSocketHub{
		key:         "jsonrpc|ws://test",
		format:      "jsonrpc",
		routeDispatchEnabled: true,
		subscribers: make(map[int]*sharedWSSubscriber),
		routeIndex:  make(map[string]map[int]struct{}),
		done:        make(chan struct{}),
	}
	subID, ch, err := hub.subscribe(2, nil)
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}

	call := &WebSocketCall{
		shareByEndpoint: true,
		subscribeReqIDs: map[string]struct{}{"req-1": {}},
		subscribeIDs:    make(map[string]struct{}),
		sharedHub:       hub,
		sharedSubID:     subID,
	}
	if ok := call.recordSubscriptionAck("req-1", "sub-owned"); !ok {
		t.Fatalf("expected ack recorded")
	}

	hub.broadcast([]byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"sub-owned","result":{"n":1}}}`))

	select {
	case <-ch:
	default:
		t.Fatalf("expected hub route bound after ack")
	}
}
