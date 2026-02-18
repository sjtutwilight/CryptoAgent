#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

GO_BIN="${GO_BIN:-/usr/local/go/bin/go}"

run_case() {
  local name="$1"
  shift
  echo "[CASE] $name"
  "$GO_BIN" test "$@"
  echo "[PASS] $name"
}

run_case "NF-01 queue finalization" ./internal/role -run 'TestQueuePipelineSinkFailureMustNotReportSuccess|TestFinalizeQueuedMessage' -count=1
run_case "NF-02 websocket bounded buffer" ./internal/caller -run 'TestBufferMessageDropOldest|TestBufferMessageDropNewest' -count=1
run_case "NF-03 backfill enqueue semantics" ./internal/handler/integrity -run 'TestSchedulerNoTargetReturnsError|TestChannelTargetTimeout|TestChannelTargetQueueFull|TestCompensationQueuePersistAndReplay|TestSchedulerDedupByKeyUntilResult|TestSequenceEnginePendingDedupAndMergedIntent|TestSequenceEngineIgnoresOutOfOrderResult|TestSequenceEngineCooldownRecovery' -count=1

echo "All gray drill cases passed."
