package caller

import (
	"testing"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

func TestBufferMessageDropOldest(t *testing.T) {
	call := &WebSocketCall{
		msgBuffer:            make([]*types.Message, 0, 2),
		msgBufferMaxMessages: 2,
		msgBufferMaxBytes:    1024,
		msgBufferDropPolicy:  "drop_oldest",
		wsBoundedBuffer:      true,
	}
	call.bufferMessage(map[string]any{"id": 1}, []byte("a"))
	call.bufferMessage(map[string]any{"id": 2}, []byte("b"))
	call.bufferMessage(map[string]any{"id": 3}, []byte("c"))

	if len(call.msgBuffer) != 2 {
		t.Fatalf("expected 2 buffered messages, got %d", len(call.msgBuffer))
	}
	if got := call.msgBuffer[0].Metadata["id"]; got != 2 {
		t.Fatalf("expected oldest message dropped, got first id=%v", got)
	}
}

func TestBufferMessageDropNewest(t *testing.T) {
	call := &WebSocketCall{
		msgBuffer:            make([]*types.Message, 0, 2),
		msgBufferMaxMessages: 2,
		msgBufferMaxBytes:    1024,
		msgBufferDropPolicy:  "drop_newest",
		wsBoundedBuffer:      true,
	}
	call.bufferMessage(map[string]any{"id": 1}, []byte("a"))
	call.bufferMessage(map[string]any{"id": 2}, []byte("b"))
	call.bufferMessage(map[string]any{"id": 3}, []byte("c"))

	if len(call.msgBuffer) != 2 {
		t.Fatalf("expected 2 buffered messages, got %d", len(call.msgBuffer))
	}
	if got := call.msgBuffer[1].Metadata["id"]; got != 2 {
		t.Fatalf("expected newest message dropped, got second id=%v", got)
	}
}
