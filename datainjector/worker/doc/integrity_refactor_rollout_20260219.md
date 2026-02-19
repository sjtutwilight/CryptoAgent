# Integrity 重构收尾与灰度观测（2026-02-19）

范围：`datainjector/worker/internal/handler/integrity`

## 1. 收尾结果（Task 6.4）

本轮已完成以下收尾项：

1. 清理旧分叉与失效状态字段（仅 integrity 模块）
- 删除 `engineState.LastSweep`（仅写不读）。
- 删除 `backfillSession.CurrentStart/CurrentEnd`（仅写不读）。
- 保留兼容路径 `checkTimeoutLegacy`，由 feature flag 控制，避免直接行为突变。

2. 统一重锚入口
- `OnSnapshotApplied` 与 sidechannel snapshot（可选）均收敛到 `applyAnchor(lastSeq, source, now)`。

3. 编排器解耦
- session 状态机迁移到 `BackfillOrchestrator`，`SequenceEngine` 仅做委托与执行壳。

4. 可观测闭环
- 新增并接入完整性核心指标：
  - `worker_integrity_expected_seq`
  - `worker_integrity_seen_max`
  - `worker_integrity_head_lag`
  - `worker_integrity_awaiting_snapshot`
  - `worker_integrity_gap_windows`
  - `worker_integrity_gap_missing_total`
  - `worker_integrity_gap_oldest_age_seconds`
- 接入已有未使用指标：
  - `worker_integrity_buffer_size`
  - `worker_integrity_duplicates_total`
- 新增结构化事件：
  - `integrity.snapshot.anchor`
  - `integrity.timeout.advance`
  - `integrity.session.state`

## 2. 灰度观测（Task 6.3）

### 2.1 观测方式

- 运行时间：2026-02-19（本地实跑，约 40s）
- 启动命令：

```bash
cd datainjector/worker
go run ./cmd/worker --config ./configs/base.yaml --roles /tmp/roles_integrity_obs.json
```

- 观测角色：仅 AAVE 两个 diff role（spot/perp）
  - `rec-binance-spot-aave-orderbook-diff`
  - `rec-binance-perp-aave-orderbook-diff`
- 临时角色文件 `/tmp/roles_integrity_obs.json` 做了两处调整：
  - `sink` 改为 `file`，避免 Kafka 外部依赖干扰观测
  - integrity 开启 flags：
    - `hard_timeout_priority_enabled=true`
    - `sidechannel_anchor_enabled=true`
    - `gap_window_metrics_enabled=true`

### 2.2 指标快照（/metrics）

观测期间采样（spot/perp 均一致趋势）：

- `worker_integrity_awaiting_snapshot = 0`
- `worker_integrity_buffer_size = 0`
- `worker_integrity_head_lag = 0`
- `worker_integrity_gap_windows = 0`
- `worker_integrity_gap_missing_total = 0`
- `worker_integrity_gap_oldest_age_seconds = 0`
- `worker_integrity_expected_seq` 与 `worker_integrity_seen_max` 持续单调推进，且 `seen_max - expected_seq = 0`

结论：在该观测窗口内链路连续，无可见 gap 积压，完整性状态机稳定推进。

### 2.3 运行证据

- 本地落盘输出（file sink）：
  - `runtime/data/integrity_obs/rec-binance-perp-aave-orderbook-diff/obs_000.jsonl`（234 行）
  - `runtime/data/integrity_obs/rec-binance-spot-aave-orderbook-diff/obs_000.jsonl`（190 行）
- 运行日志可见 role 生命周期与 pipeline 正常完成；退出时 SIGINT 优雅关闭。

### 2.4 告警阈值建议（首版）

建议作为值班初始阈值，后续按生产分位数校准：

1. `worker_integrity_head_lag > 50` 持续 `60s`：P2（恢复压力上升）
2. `worker_integrity_gap_windows > 0` 持续 `30s`：P2（存在未闭合缺口）
3. `worker_integrity_gap_oldest_age_seconds > 15`：P1（恢复停滞风险）
4. `worker_integrity_awaiting_snapshot = 1` 持续 `> hard_timeout`：P1（快照等待异常）
5. `worker_integrity_duplicates_total` 斜率异常升高：P3（上游重复或幂等键异常）

## 3. Feature Flag 清单（默认兼容）

`integrity.with` 新增（默认全 `false`）：

- `hard_timeout_priority_enabled`
- `sidechannel_anchor_enabled`
- `gap_window_metrics_enabled`

默认关闭时保持旧行为；灰度建议按 role 分批开启。

