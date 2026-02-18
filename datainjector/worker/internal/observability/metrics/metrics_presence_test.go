package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsEndpointContainsWorkerCutoverMetrics(t *testing.T) {
	// 先写入样本，确保向量指标在 exposition 中可见。
	RecordBackfillResult("role-a", "range", "success", "none")
	RecordBackfillPendingDuration("role-a", "stream-a", "range", "success", 2*time.Second)
	SetBackfillSessionsInflight("role-a", "stream-a", "range", 1)
	RecordBackfillScheduleDedup("role-a", "stream-a", "range")
	RecordBackfillEnqueue("role-a", "range", "success", 100*time.Millisecond)
	SetBackfillCompensationBacklog("role-a", 3)
	RecordTaskStage("role-a", "caller_accepted", "event")
	RecordWebSocketDrop("role-a", "caller_buffer", "max_messages_drop_oldest")
	RecordOrderbookSnapshotEmitted("role-a", "periodic", "periodic")

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	required := []string{
		"worker_integrity_backfill_result_total",
		"worker_integrity_backfill_pending_duration_seconds",
		"worker_integrity_backfill_sessions_inflight",
		"worker_integrity_backfill_schedule_dedup_total",
		"worker_integrity_backfill_enqueue_latency_seconds",
		"worker_integrity_backfill_compensation_backlog",
		"worker_task_stage_total",
		"worker_websocket_drops_total",
		"worker_orderbook_snapshot_emitted_total",
	}

	for _, metricName := range required {
		if !strings.Contains(body, metricName) {
			t.Fatalf("expected metric %s in /metrics output", metricName)
		}
	}
}
