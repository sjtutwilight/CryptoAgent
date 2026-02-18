## 1. 事件模型与规则基线

- [x] 1.1 在 `worker/internal/observability/logging/events.go` 增加完整性/backfill事件常量（gap触发、backfill触发、attempt失败、成功、exhausted）
- [x] 1.2 更新 `worker/LOGGING_SPEC.md` 事件字典，补齐新增事件字段与触发点
- [x] 1.3 定义故障回归默认规则配置（must_events、fail_events、恢复窗口、模式参数）并放入 `automation/test` 可加载位置

## 2. Worker 结构化埋点实现

- [x] 2.1 在 `worker/internal/handler/integrity/sequence_engine.go` 将关键 `log.Printf` 替换/补充为结构化事件输出
- [x] 2.2 在 `worker/internal/role/role.go` 的 backfill 执行路径增加结构化成功/失败/耗尽事件并携带 `role_id`
- [x] 2.3 对新增事件补充单元测试或行为测试，验证事件名与关键字段输出正确

## 3. 故障回归 Probe 与判定器

- [x] 3.1 在 `automation/test/probes/worker_probe.py` 增加按 `role_id` 聚合事件与缺失事件计算能力
- [x] 3.2 实现断连重连与补数恢复判定函数（含失败事件短路）
- [x] 3.3 增加结构化优先、文本兜底的兼容路径并在结果中标注命中来源

## 4. 场景编排与注入执行

- [x] 4.1 新增 `automation/test/scenarios/datainjector_fault_regression.py`，按 prepare/inject/observe/assert/report 分阶段编排
- [x] 4.2 实现 mock 模式注入流程（调用 mockDataProvider 配置与统计接口）
- [x] 4.3 实现 real 模式注入流程（容器 pause/unpause）并确保结束后自动恢复
- [x] 4.4 将新场景注册到 `automation/test/tools/run_scenario.py` 与 `tool/test.sh list` 可见路径

## 5. 报告产物与运行集成

- [x] 5.1 在 `automation/test/runs/<run_id>/` 生成 `summary.json`（机器可读总体/按role结果）
- [x] 5.2 生成 `summary.txt`（人工可读摘要）与 `evidence.jsonl`（事件证据流）
- [x] 5.3 在场景结束时回写生效规则与窗口参数，保证结果可复现

## 6. 联调与验收

- [x] 6.1 使用 `integrity_clean` 场景执行一次基线回归，确认全 role PASS
- [x] 6.2 使用 `integrity_gap` 与断连注入执行异常回归，确认缺失事件与失败证据输出正确
- [x] 6.3 补充文档（如何运行、参数说明、结果解读）并记录已知限制
