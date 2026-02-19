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
	SetIntegrityBufferSize("role-a", "stream-a", 12)
	RecordIntegrityDuplicate("role-a", "stream-a")
	SetIntegrityExpectedSeq("role-a", "stream-a", 101)
	SetIntegritySeenMax("role-a", "stream-a", 120)
	SetIntegrityHeadLag("role-a", "stream-a", 19)
	SetIntegrityAwaitingSnapshot("role-a", "stream-a", true)
	SetIntegrityGapWindows("role-a", "stream-a", 2)
	SetIntegrityGapMissingTotal("role-a", "stream-a", 7)
	SetIntegrityGapOldestAge("role-a", "stream-a", 3*time.Second)

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
		"worker_integrity_buffer_size",
		"worker_integrity_duplicates_total",
		"worker_integrity_expected_seq",
		"worker_integrity_seen_max",
		"worker_integrity_head_lag",
		"worker_integrity_awaiting_snapshot",
		"worker_integrity_gap_windows",
		"worker_integrity_gap_missing_total",
		"worker_integrity_gap_oldest_age_seconds",
	}

	for _, metricName := range required {
		if !strings.Contains(body, metricName) {
			t.Fatalf("expected metric %s in /metrics output", metricName)
		}
	}
}
