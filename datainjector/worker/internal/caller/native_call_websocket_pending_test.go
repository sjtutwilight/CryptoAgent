package caller

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDeliverPendingResponseResult(t *testing.T) {
	respCh := make(chan wsRPCResponse, 1)
	call := &WebSocketCall{
		wsPending: map[string]chan wsRPCResponse{
			"req-1": respCh,
		},
	}

	if ok := call.deliverPendingResponse(json.RawMessage(`"req-1"`), json.RawMessage(`{"ok":true}`), nil); !ok {
		t.Fatalf("expected pending response delivered")
	}
	if _, exists := call.wsPending["req-1"]; exists {
		t.Fatalf("expected pending entry removed after delivery")
	}

	select {
	case resp := <-respCh:
		if resp.err != nil {
			t.Fatalf("expected nil error, got %v", resp.err)
		}
		if got := string(resp.result); got != `{"ok":true}` {
			t.Fatalf("unexpected result payload: %s", got)
		}
	default:
		t.Fatalf("expected response written to pending channel")
	}
}

func TestDeliverPendingResponseError(t *testing.T) {
	respCh := make(chan wsRPCResponse, 1)
	call := &WebSocketCall{
		wsPending: map[string]chan wsRPCResponse{
			"2": respCh,
		},
	}

	errPayload := &struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{Code: -32000, Message: "boom"}

	if ok := call.deliverPendingResponse(json.RawMessage(`2`), nil, errPayload); !ok {
		t.Fatalf("expected numeric id pending response delivered")
	}

	select {
	case resp := <-respCh:
		if resp.err == nil {
			t.Fatalf("expected rpc error response")
		}
		if !strings.Contains(resp.err.Error(), "code=-32000") {
			t.Fatalf("expected rpc error code in message, got %v", resp.err)
		}
	default:
		t.Fatalf("expected error response written to pending channel")
	}
}

func TestHandleJSONRPCMessagePendingResponseInSharedMode(t *testing.T) {
	respCh := make(chan wsRPCResponse, 1)
	call := &WebSocketCall{
		shareByEndpoint: true,
		notifyMethod:    "eth_subscription",
		wsPending: map[string]chan wsRPCResponse{
			"foreign-req": respCh,
		},
		subscribeReqIDs: make(map[string]struct{}),
		subscribeIDs:    make(map[string]struct{}),
	}

	if err := call.handleJSONRPCMessage([]byte(`{"jsonrpc":"2.0","id":"foreign-req","error":{"code":-32000,"message":"boom"}}`)); err != nil {
		t.Fatalf("expected pending rpc response handled before shared filtering, got %v", err)
	}

	select {
	case resp := <-respCh:
		if resp.err == nil {
			t.Fatalf("expected pending channel to receive rpc error")
		}
	default:
		t.Fatalf("expected pending response delivered to waiting caller")
	}
}
