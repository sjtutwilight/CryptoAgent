## ADDED Requirements

### Requirement: Integrity Sequencer SHALL expose pure state transition contract
完整性模块 MUST 提供不依赖 IO 的状态机核心接口，输入为标准化事件与当前状态，输出为新状态与可执行动作列表。

#### Scenario: Sequence event generates deterministic actions
- **WHEN** 输入相同的状态快照与相同事件序列
- **THEN** 状态机 SHALL 产出一致的新状态与动作顺序，不依赖外部时序副作用

#### Scenario: Core does not perform side effects directly
- **WHEN** 状态机判定需要触发 backfill 或推进 expected
- **THEN** 状态机 MUST 仅返回动作描述，不直接执行调度、日志或指标写入

### Requirement: Snapshot anchor semantics MUST be unified across modes
系统 MUST 通过统一重锚动作处理 snapshot 应用，避免 `snapshot_gate` 与 `snapshot_sidechannel` 语义分叉。

#### Scenario: Snapshot applied callback re-anchors sequence
- **WHEN** 系统接收到 `OnSnapshotApplied(lastSeq)`
- **THEN** 引擎 SHALL 统一更新 `ExpectedNext` 与缓冲清理边界到 `lastSeq` 对齐状态

#### Scenario: Sidechannel snapshot with anchor updates local state
- **WHEN** sidechannel 模式收到可解析锚点序列的 snapshot 事件
- **THEN** 引擎 MUST 执行与 gate 模式一致的重锚语义，而非仅透传事件

### Requirement: Hard timeout SHALL take precedence over soft timeout
系统 MUST 在超时判定中优先执行 `hard_timeout` 推进逻辑，再执行 `max_delay` 补数尝试逻辑。

#### Scenario: Hard timeout prevents indefinite stall
- **WHEN** gap 长时间存在且 `max_delay` 持续命中
- **THEN** 引擎 MUST 在 `hard_timeout` 到达后执行推进，不得被软超时路径长期阻断

#### Scenario: Soft timeout still triggers recovery attempt
- **WHEN** 尚未达到 `hard_timeout` 但已超过 `max_delay`
- **THEN** 引擎 SHALL 触发受控 backfill 尝试并保留后续硬超时推进机会

### Requirement: Gap recovery SHALL maintain an explicit missing-window view
系统 MUST 维护可查询的缺失窗口视图，至少包含窗口数量、总缺失长度与最老缺口年龄。

#### Scenario: Gap appears and updates missing-window metrics
- **WHEN** 检测到新 gap 并进入等待恢复状态
- **THEN** 系统 SHALL 更新缺失窗口视图并驱动对应观测指标

#### Scenario: Anchor or advance shrinks missing-window view
- **WHEN** 发生 snapshot 重锚或 hard-timeout 推进
- **THEN** 系统 MUST 同步收敛缺失窗口，保证窗口视图与 `ExpectedNext` 一致
