# Codex 事件到效率指标映射口径

## 1. 口径目标

本口径用于统一度量 Codex 决策链效率，覆盖以下四类核心问题：

1. 高频重复步骤（可脚本化候选）
2. 低价值读取（读多改少测少）
3. 工具失败热点（tool_error）
4. 首个有效产出时延（session_start -> patch_apply/tool_result）

## 2. 事件标准字段

- `event_name`：事件名（示例：`session_start`、`tool_decision`、`tool_error`、`patch_apply`）
- `session_id`：会话唯一标识
- `tool_name`：工具名
- `command`：命令/动作描述
- `outcome`：执行结果（ok/success/error 等）
- `timestamp`：事件时间（秒）

## 3. 指标定义

### 3.1 高频重复步骤

- 指标：`codex_repeat_step_count`
- 规则：按 `session_id` 分组，`event_name + tool_name + command` 重复出现次数 `N>1`，累计 `N-1`
- 解释：值越高，脚本化收益越高

### 3.2 低价值读取比

- 指标：`codex_low_value_read_ratio`
- 公式：`read_count / max(edit_count + test_count, 1)`
- 解释：值越高，说明读取行为没有有效转化为改动/验证

### 3.3 工具失败热点

- 指标：`codex_tool_error_total`
- 补充：按 `tool_name` 聚合 topN 输出热点列表
- 解释：用于定位最先治理的高失败工具

### 3.4 首个有效产出时延

- 指标：`codex_first_effective_output_latency_seconds`
- 规则：同一 `session_id` 下，`session_start` 到首次 `patch_apply` 或 `tool_result(success)` 的时差
- 解释：值越低，表示决策链更高效

## 4. 候选建议生成规则

- 脚本候选：`codex_repeat_step_count >= 5`
- 文档候选：`codex_low_value_read_ratio >= 3.0`
- 流程候选：`codex_tool_error_total >= 10`

## 5. 审核与回归

- 建议状态：`proposed -> approved -> implemented`
- 回归结论：`improved | neutral | regressed`
- 若 `regressed`：自动生成复盘模板并进入修正流程
