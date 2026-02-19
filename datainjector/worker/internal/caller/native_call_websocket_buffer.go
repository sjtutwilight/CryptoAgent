package caller

import (
	"context"

	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/logging"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/observability/metrics"
	"github.com/twilight-labs/dataplatform/datainjector/worker/internal/types"
)

func (w *WebSocketCall) bufferMessage(meta map[string]any, data []byte) {
	if meta == nil {
		meta = make(map[string]any)
	}
	payload := make([]byte, len(data))
	copy(payload, data)
	msg := &types.Message{Metadata: meta, Payload: payload}
	dropReason := ""
	dropIncoming := false
	bufferSize := 0
	bufferBytes := 0
	roleID := ""
	subscription := ""
	maxMessages := 0
	maxBytes := 0

	w.mu.Lock()
	msgSize := len(payload)
	effectivePolicy := w.msgBufferDropPolicy
	if w.messageBackpressure {
		effectivePolicy = "drop_newest"
	}
	if w.wsBoundedBuffer && w.msgBufferMaxMessages > 0 && len(w.msgBuffer) >= w.msgBufferMaxMessages {
		if effectivePolicy == "drop_newest" {
			dropIncoming = true
			dropReason = "max_messages_drop_newest"
		} else {
			dropReason = "max_messages_drop_oldest"
			w.dropOldestBufferedMessageLocked()
		}
	}

	if !dropIncoming && w.wsBoundedBuffer && w.msgBufferMaxBytes > 0 && msgSize > w.msgBufferMaxBytes {
		dropIncoming = true
		dropReason = "message_too_large"
	}

	if !dropIncoming && w.wsBoundedBuffer && w.msgBufferMaxBytes > 0 && w.msgBufferBytes+msgSize > w.msgBufferMaxBytes {
		if effectivePolicy == "drop_newest" {
			dropIncoming = true
			dropReason = "max_bytes_drop_newest"
		} else {
			for len(w.msgBuffer) > 0 && w.msgBufferBytes+msgSize > w.msgBufferMaxBytes {
				w.dropOldestBufferedMessageLocked()
			}
			if w.msgBufferBytes+msgSize > w.msgBufferMaxBytes {
				dropIncoming = true
				dropReason = "max_bytes_drop_newest"
			} else if dropReason == "" {
				dropReason = "max_bytes_drop_oldest"
			}
		}
	}

	if !dropIncoming {
		w.msgBuffer = append(w.msgBuffer, msg)
		w.msgBufferBytes += msgSize
	}
	bufferSize = len(w.msgBuffer)
	bufferBytes = w.msgBufferBytes
	roleID = w.roleID
	subscription = w.subscribeTopic
	maxMessages = w.msgBufferMaxMessages
	maxBytes = w.msgBufferMaxBytes
	w.mu.Unlock()

	if dropReason != "" {
		logging.Warn(context.Background(), logging.EventWSBufferDrop, "websocket caller buffer drop", logging.Fields{
			"role_id":       roleID,
			"subscription":  subscription,
			"buffer_layer":  "caller_buffer",
			"drop_reason":   dropReason,
			"drop_policy":   effectivePolicy,
			"buffer_size":   bufferSize,
			"buffer_bytes":  bufferBytes,
			"max_messages":  maxMessages,
			"max_bytes":     maxBytes,
			"message_bytes": msgSize,
		})
		metrics.RecordWebSocketDrop(roleID, "caller_buffer", dropReason)
	}
}

func (w *WebSocketCall) dropOldestBufferedMessageLocked() {
	if len(w.msgBuffer) == 0 {
		return
	}
	removed := w.msgBuffer[0]
	w.msgBuffer[0] = nil
	w.msgBuffer = w.msgBuffer[1:]
	if removed != nil {
		w.msgBufferBytes -= len(removed.Payload)
		if w.msgBufferBytes < 0 {
			w.msgBufferBytes = 0
		}
	}
}

func (w *WebSocketCall) refreshMessageBackpressure(ch <-chan []byte) {
	capacity := cap(ch)
	if capacity <= 0 {
		w.mu.Lock()
		w.messageBackpressure = false
		w.mu.Unlock()
		return
	}
	usedPercent := (len(ch) * 100) / capacity
	high := w.backpressureHighPct
	low := w.backpressureLowPct
	if high <= 0 {
		high = 80
	}
	if low < 0 || low >= high {
		low = high / 2
	}

	logEnter := false
	logExit := false
	roleID := ""
	subscription := ""
	wsBounded := false
	w.mu.Lock()
	roleID = w.roleID
	subscription = w.subscribeTopic
	wsBounded = w.wsBoundedBuffer
	if !w.messageBackpressure {
		if usedPercent >= high {
			w.messageBackpressure = true
			logEnter = true
		}
	} else if usedPercent <= low {
		w.messageBackpressure = false
		logExit = true
	}
	w.mu.Unlock()

	if logEnter {
		logging.Warn(context.Background(), logging.EventWSBufferDrop, "websocket enters backpressure mode", logging.Fields{
			"role_id":         roleID,
			"subscription":    subscription,
			"buffer_layer":    "upstream_channel",
			"drop_reason":     "backpressure_high_watermark",
			"used_percent":    usedPercent,
			"high_watermark":  high,
			"load_shedding":   "prefer_drop_newest",
			"ws_bounded_mode": wsBounded,
		})
		return
	}
	if logExit {
		logging.Info(context.Background(), logging.EventWSSubscribeAck, "websocket exits backpressure mode", logging.Fields{
			"role_id":       roleID,
			"subscription":  subscription,
			"used_percent":  usedPercent,
			"low_watermark": low,
			"buffer_layer":  "upstream_channel",
			"drop_reason":   "backpressure_recovered",
		})
	}
}
