---
name: openspec-sync-specs
description: 将某个变更中的 delta specs 同步到主 specs。当用户希望在不归档 change 的情况下更新主 specs 时使用。
license: MIT
compatibility: 需要 openspec CLI。
metadata:
  author: openspec
  version: "1.0"
  generatedBy: "1.1.1"
---

将某个 change 的 delta specs 同步到主 specs。

这是一个 **agent 驱动**操作：你会读取 delta specs，并直接编辑主 specs 以应用变更。这样可以实现智能合并（例如只新增一个 scenario，而不复制整个 requirement）。

**输入**：可选指定 change 名称。若省略，检查是否可从对话上下文推断。若含糊或有歧义，你必须提示可用 change。

**步骤**

1. **若未提供 change 名称，提示用户选择**

   运行 `openspec list --json` 获取可用 change。使用 **AskUserQuestion 工具**让用户选择。

   仅展示带有 delta specs 的 change（在 `specs/` 目录下）。

   **重要**：不要猜测或自动选择 change。必须由用户选择。

2. **查找 delta specs**

   在 `openspec/changes/<name>/specs/*/spec.md` 中查找 delta spec 文件。

   每个 delta spec 文件通常包含以下区块：
   - `## ADDED Requirements` - 要新增的新 requirement
   - `## MODIFIED Requirements` - 对已有 requirement 的修改
   - `## REMOVED Requirements` - 要删除的 requirement
   - `## RENAMED Requirements` - 要重命名的 requirement（FROM:/TO: 格式）

   若未发现 delta specs，告知用户并停止。

3. **对每个 delta spec，将变更应用到主 specs**

   对每个 capability（delta spec 路径为 `openspec/changes/<name>/specs/<capability>/spec.md`）：

   a. **读取 delta spec**，理解其意图变更

   b. **读取主 spec**：`openspec/specs/<capability>/spec.md`（可能不存在）

   c. **智能应用变更**：

      **ADDED Requirements：**
      - 若主 spec 中不存在该 requirement → 新增
      - 若主 spec 中已存在该 requirement → 更新为 delta 内容（视为隐式 MODIFIED）

      **MODIFIED Requirements：**
      - 在主 spec 中找到该 requirement
      - 应用变更，可能包括：
        - 新增 scenarios（不需要复制原有 scenarios）
        - 修改已有 scenarios
        - 修改 requirement 描述
      - 保留 delta 中未提及的 scenarios/内容

      **REMOVED Requirements：**
      - 从主 spec 中移除整个 requirement 区块

      **RENAMED Requirements：**
      - 找到 FROM requirement，重命名为 TO

   d. **若 capability 的主 spec 不存在，创建新主 spec**：
      - 创建 `openspec/specs/<capability>/spec.md`
      - 添加 Purpose 区块（可简短，标记为 TBD）
      - 在 Requirements 区块加入 ADDED requirements

4. **展示总结**

   所有变更应用后，汇总：
   - 更新了哪些 capability
   - 做了哪些变更（新增/修改/删除/重命名 requirement）

**Delta Spec 格式参考**

```markdown
## ADDED Requirements

### Requirement: New Feature
The system SHALL do something new.

#### Scenario: Basic case
- **WHEN** user does X
- **THEN** system does Y

## MODIFIED Requirements

### Requirement: Existing Feature
#### Scenario: New scenario to add
- **WHEN** user does A
- **THEN** system does B

## REMOVED Requirements

### Requirement: Deprecated Feature

## RENAMED Requirements

- FROM: `### Requirement: Old Name`
- TO: `### Requirement: New Name`
```

**关键原则：智能合并**

与程序化覆盖不同，你可以做 **部分更新**：
- 若只想新增一个 scenario，只需在 MODIFIED 下写该 scenario，不需要复制原有 scenarios
- delta 表示的是*意图*，不是整段替换
- 使用你的判断做合理合并

**成功输出示例**

```
## Specs Synced: <change-name>

Updated main specs:

**<capability-1>**:
- Added requirement: "New Feature"
- Modified requirement: "Existing Feature" (added 1 scenario)

**<capability-2>**:
- Created new spec file
- Added requirement: "Another Feature"

Main specs are now updated. The change remains active - archive when implementation is complete.
```

**护栏**
- 改动前必须同时读取 delta 与主 spec
- 保留 delta 未提及的已有内容
- 不清晰之处先询问
- 变更过程中要展示正在改什么
- 操作应尽量幂等：重复执行两次结果应一致
