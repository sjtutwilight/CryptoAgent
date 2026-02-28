## ADDED Requirements

### Requirement: 系统必须输出可执行的优化建议
系统 MUST 将分析结果转化为可执行建议，并按类型区分为脚本候选、文档候选、流程候选。

#### Scenario: 建议包含行动项
- **WHEN** 生成任一优化建议
- **THEN** 建议内容 MUST 包含目标问题、建议动作、预期收益与影响范围

#### Scenario: 建议可追溯到观测证据
- **WHEN** 审核者查看建议详情
- **THEN** 系统 MUST 提供对应事件统计与样本引用

### Requirement: 建议落地必须经过人工审核门禁
系统 MUST 为优化建议提供审核状态流转，未经审核通过不得进入自动执行阶段。

#### Scenario: 未审核建议不得执行
- **WHEN** 建议状态为 `proposed`
- **THEN** 系统 MUST 禁止进入执行或自动改造步骤

#### Scenario: 审核轨迹可审计
- **WHEN** 建议状态发生变更（如 `approved`、`rejected`、`implemented`）
- **THEN** 系统 MUST 记录操作者、时间和变更原因

### Requirement: 落地后必须进行效果回归验证
系统 MUST 对已实施建议进行前后指标对比，验证是否达到预期收益。

#### Scenario: 执行后自动生成回归结论
- **WHEN** 建议状态进入 `implemented`
- **THEN** 系统 MUST 在约定观察窗口结束后生成对比结论（improved / neutral / regressed）

#### Scenario: 回归失败触发复盘
- **WHEN** 回归结论为 `regressed`
- **THEN** 系统 MUST 触发复盘任务并要求补充修正动作
